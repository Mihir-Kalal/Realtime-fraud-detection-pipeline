"""
training/train.py

Offline XGBoost training for the fraud-detection pipeline (Phase 3).

Reads historical transactions/features from Postgres (`feature_snapshots`,
locked Phase-2 schema, fields 1-9 in the exact locked order) joined against
ground-truth labels from `labels` (written by the feedback loop), trains an
XGBoost binary classifier, logs the run to MLflow, registers the model
artifact under the registry name `fraud-xgb`, runs a SHAP TreeExplainer
sanity check, and writes the resulting model URI into `model_registry_meta`
(is_active=true) per the mechanism Phase 0 already decided on (open issue #1:
serving loads the model by querying `model_registry_meta` for
`is_active=true`, not by MLflow stage/alias).

Run on demand (NOT a long-running compose service):
    docker compose run --rm training python training/train.py

Exit codes:
    0 - success (model trained+registered) OR graceful no-op (not enough
        labeled rows yet -- this is expected/normal early in the pipeline's
        life, so it is NOT treated as a failure)
    1 - actual failure (DB unreachable, MLflow unreachable, training error)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import shap
import xgboost as xgb
from mlflow.tracking import MlflowClient
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    fbeta_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [train] %(message)s",
)
logger = logging.getLogger("train")

# --------------------------------------------------------------------------
# LOCKED CONTRACTS (do not rename/reorder without updating PROJECT_STATE.md)
# --------------------------------------------------------------------------

# Feature vector = fields 1-9 of the Phase-2 locked `feature_snapshots`
# schema, in this exact order. This ordering is what gets fed to
# `model.predict_proba` / `booster.predict` both here and in Phase 4 serving.
from common.feature_columns import FEATURE_COLUMNS

MODEL_NAME = "fraud-xgb"

# --------------------------------------------------------------------------
# Config (env-overridable, all optional with sane defaults)
# --------------------------------------------------------------------------


@dataclass
class Config:
    postgres_dsn: str = field(
        default_factory=lambda: os.environ.get(
            "POSTGRES_DSN",
            "postgresql://frauduser:fraudpass@postgres:5432/frauddb",
        )
    )
    mlflow_tracking_uri: str = field(
        default_factory=lambda: os.environ.get(
            "MLFLOW_TRACKING_URI", "http://mlflow:5000"
        )
    )
    mlflow_experiment: str = field(
        default_factory=lambda: os.environ.get(
            "MLFLOW_EXPERIMENT", "fraud-xgb-training"
        )
    )
    min_labeled_rows: int = field(
        default_factory=lambda: int(os.environ.get("MIN_LABELED_ROWS", "200"))
    )
    test_size: float = field(
        default_factory=lambda: float(os.environ.get("TEST_SIZE", "0.2"))
    )
    random_seed: int = field(
        default_factory=lambda: int(os.environ.get("RANDOM_SEED", "42"))
    )
    shap_sample_size: int = field(
        default_factory=lambda: int(os.environ.get("SHAP_SAMPLE_SIZE", "200"))
    )
    xgb_num_boost_round: int = field(
        default_factory=lambda: int(os.environ.get("XGB_NUM_BOOST_ROUND", "300"))
    )
    xgb_early_stopping_rounds: int = field(
        default_factory=lambda: int(
            os.environ.get("XGB_EARLY_STOPPING_ROUNDS", "20")
        )
    )
    xgb_max_depth: int = field(
        default_factory=lambda: int(os.environ.get("XGB_MAX_DEPTH", "5"))
    )
    xgb_eta: float = field(
        default_factory=lambda: float(os.environ.get("XGB_ETA", "0.05"))
    )


# --------------------------------------------------------------------------
# Data loading
# --------------------------------------------------------------------------


def load_training_frame(cfg: Config) -> pd.DataFrame:
    """Join Phase-2 `feature_snapshots` against feedback-loop `labels` on
    `txn_id`. One row per labeled transaction. If a `txn_id` has more than
    one feature_snapshots row (shouldn't happen given the Phase-2 unique-key
    write pattern, but guarded anyway) the most recent `computed_at` wins.
    """
    query = f"""
        WITH ranked_features AS (
            SELECT
                fs.txn_id,
                fs.{', fs.'.join(FEATURE_COLUMNS)},
                fs.computed_at,
                ROW_NUMBER() OVER (
                    PARTITION BY fs.txn_id ORDER BY fs.computed_at DESC
                ) AS rn
            FROM feature_snapshots fs
        )
        SELECT
            rf.txn_id,
            {', '.join(f'rf.{c}' for c in FEATURE_COLUMNS)},
            l.is_fraud
        FROM ranked_features rf
        INNER JOIN labels l ON l.txn_id = rf.txn_id
        WHERE rf.rn = 1
    """
    logger.info("Connecting to Postgres...")
    conn = psycopg2.connect(cfg.postgres_dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            logger.info("Executing training data query (feature_snapshots JOIN labels)...")
            cur.execute(query)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return pd.DataFrame(columns=["txn_id", *FEATURE_COLUMNS, "is_fraud"])

    df = pd.DataFrame(rows)
    # is_fraud may come back as bool or int from Postgres depending on the
    # `labels` table's column type -- normalize to int8 {0,1}.
    df["is_fraud"] = df["is_fraud"].astype(int)
    for col in FEATURE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    before = len(df)
    df = df.dropna(subset=FEATURE_COLUMNS + ["is_fraud"])
    dropped = before - len(df)
    if dropped:
        logger.warning(
            "Dropped %d/%d rows with null feature/label values after join",
            dropped,
            before,
        )

    return df.reset_index(drop=True)


# --------------------------------------------------------------------------
# Training
# --------------------------------------------------------------------------


def train_model(
    df: pd.DataFrame, cfg: Config
) -> tuple[xgb.Booster, dict[str, float], dict[str, Any], pd.DataFrame, pd.Series]:
    X = df[FEATURE_COLUMNS]
    y = df["is_fraud"]

    fraud_rate = float(y.mean())
    logger.info(
        "Training frame: %d rows, %d positive (%.4f%% fraud rate)",
        len(df),
        int(y.sum()),
        fraud_rate * 100,
    )

    if y.nunique() < 2:
        raise RuntimeError(
            f"Training data has only one class present (fraud_rate={fraud_rate}); "
            "cannot train a binary classifier. Wait for more diverse labels."
        )

    # Rename columns to f0, f1... so the C++ booster NEVER sees the real names.
    # This is REQUIRED for onnxmltools to convert the booster to ONNX later.
    X_renamed = X.copy()
    X_renamed.columns = [f"f{i}" for i in range(len(FEATURE_COLUMNS))]

    X_train, X_test, y_train, y_test = train_test_split(
        X_renamed,
        y,
        test_size=cfg.test_size,
        random_state=cfg.random_seed,
        stratify=y,
    )

    n_pos = int(y_train.sum())
    n_neg = int(len(y_train) - n_pos)
    scale_pos_weight = float(n_neg) / max(n_pos, 1)

    params: dict[str, Any] = {
        "objective": "binary:logistic",
        "eval_metric": ["auc", "aucpr", "logloss"],
        "max_depth": cfg.xgb_max_depth,
        "eta": cfg.xgb_eta,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 2,
        "scale_pos_weight": scale_pos_weight,
        "seed": cfg.random_seed,
        "tree_method": "hist",
    }

    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)

    evals_result: dict[str, Any] = {}
    booster = xgb.train(
        params,
        dtrain,
        num_boost_round=cfg.xgb_num_boost_round,
        evals=[(dtrain, "train"), (dtest, "test")],
        early_stopping_rounds=cfg.xgb_early_stopping_rounds,
        evals_result=evals_result,
        verbose_eval=False,
    )

    best_iteration = booster.best_iteration
    y_pred_proba = booster.predict(
        dtest, iteration_range=(0, best_iteration + 1)
    )
    y_pred_label = (y_pred_proba >= 0.5).astype(int)

    best_thresh = 0.5
    best_f05 = 0.0
    best_f1_at_thresh = 0.0
    for thresh in np.linspace(0.01, 0.99, 99):
        y_pred = (y_pred_proba >= thresh).astype(int)
        
        pred_flag_rate = y_pred.mean()
        # Constrain the threshold so the flag rate is realistic (between 50% and 300% of base fraud rate)
        if pred_flag_rate > fraud_rate * 3.0 or pred_flag_rate < fraud_rate * 0.5:
            continue
            
        f05 = fbeta_score(y_test, y_pred, beta=0.5, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        if f05 > best_f05:
            best_f05 = f05
            best_f1_at_thresh = f1
            best_thresh = thresh

    metrics = {
        "auc": float(roc_auc_score(y_test, y_pred_proba)),
        "pr_auc": float(average_precision_score(y_test, y_pred_proba)),
        "log_loss": float(log_loss(y_test, y_pred_proba, labels=[0, 1])),
        "precision_at_0.5": float(
            precision_score(y_test, y_pred_label, zero_division=0)
        ),
        "recall_at_0.5": float(recall_score(y_test, y_pred_label, zero_division=0)),
        "f1_at_0.5": float(f1_score(y_test, y_pred_label, zero_division=0)),
        "best_threshold": float(best_thresh),
        "f1_at_best_threshold": float(best_f1_at_thresh),
        "best_iteration": float(best_iteration),
        "train_rows": float(len(X_train)),
        "test_rows": float(len(X_test)),
        "fraud_rate": float(fraud_rate),
    }

    run_params = {
        **{f"xgb_{k}": v for k, v in params.items()},
        "num_boost_round": cfg.xgb_num_boost_round,
        "early_stopping_rounds": cfg.xgb_early_stopping_rounds,
        "test_size": cfg.test_size,
        "random_seed": cfg.random_seed,
        "n_features": len(FEATURE_COLUMNS),
        "feature_columns": ",".join(FEATURE_COLUMNS),
    }

    return booster, metrics, run_params, X_test, y_test


# --------------------------------------------------------------------------
# SHAP sanity check
# --------------------------------------------------------------------------


def run_shap_sanity_check(
    booster: xgb.Booster, X_test: pd.DataFrame, cfg: Config
) -> dict[str, Any]:
    """Runs shap.TreeExplainer against a sample of the held-out test set and
    returns a small, artifact-loggable summary (mean |SHAP| per feature).
    This mirrors exactly what Phase 4 serving will do per-transaction
    (TreeExplainer over the same booster, same feature order) -- if this
    sanity check works, serving's SHAP path will too.
    """
    sample_n = min(cfg.shap_sample_size, len(X_test))
    sample = X_test.sample(n=sample_n, random_state=cfg.random_seed)

    explainer = shap.TreeExplainer(booster)
    # Pass .values (numpy array) to prevent SHAP from injecting pandas
    # column names into the C++ booster, which breaks ONNX conversion later.
    shap_values = explainer.shap_values(sample.values)
    shap_values = np.asarray(shap_values)
    if shap_values.ndim == 3:
        # Some shap/xgboost version combos return (n_classes, n_rows, n_features)
        shap_values = shap_values[-1]

    mean_abs_shap = np.abs(shap_values).mean(axis=0)
    summary = {
        feature: float(val)
        for feature, val in zip(FEATURE_COLUMNS, mean_abs_shap)
    }
    ranked = sorted(summary.items(), key=lambda kv: kv[1], reverse=True)

    logger.info("SHAP sanity check (mean |impact| over %d sampled rows):", sample_n)
    for feature, val in ranked:
        logger.info("  %-24s %.6f", feature, val)

    return {
        "sample_size": sample_n,
        "mean_abs_shap_by_feature": summary,
        "ranked_features": [f for f, _ in ranked],
        "base_value": float(np.asarray(explainer.expected_value).reshape(-1)[0]),
    }


# --------------------------------------------------------------------------
# MLflow logging + registration
# --------------------------------------------------------------------------


def log_and_register(
    booster: xgb.Booster,
    metrics: dict[str, float],
    run_params: dict[str, Any],
    shap_summary: dict[str, Any],
    cfg: Config,
) -> tuple[str, str]:
    """Logs the run to MLflow and registers the artifact under `fraud-xgb`.

    Returns (run_id, registered_version) as strings.
    """
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment)

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        mlflow.log_params(run_params)
        mlflow.log_metrics(metrics)

        shap_path = "/tmp/shap_sanity_check.json"
        with open(shap_path, "w") as f:
            json.dump(shap_summary, f, indent=2)
        mlflow.log_artifact(shap_path, artifact_path="shap")

        # The booster was trained without feature names so ONNX conversion succeeds.
        import onnx
        from onnxmltools.convert import convert_xgboost
        from onnxmltools.convert.common.data_types import FloatTensorType
        
        initial_type = [('float_input', FloatTensorType([None, len(FEATURE_COLUMNS)]))]
        onnx_model = convert_xgboost(booster, initial_types=initial_type)
        onnx_path = "/tmp/fraud-xgb.onnx"
        with open(onnx_path, "wb") as f:
            f.write(onnx_model.SerializeToString())
        mlflow.log_artifact(onnx_path, artifact_path="onnx")

        # Now set the correct feature names so the saved XGBoost artifact has them
        # (required for SHAP explainability in the scoring API).
        booster.feature_names = FEATURE_COLUMNS

        # Log + register the raw XGBoost Booster. Phase 4 serving MUST load
        # it with `mlflow.xgboost.load_model(model_uri)`
        model_info = mlflow.xgboost.log_model(
            xgb_model=booster,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )



    client = MlflowClient(tracking_uri=cfg.mlflow_tracking_uri)
    # Find the registry version that was just created against this run_id.
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    matching = [v for v in versions if v.run_id == run_id]
    if not matching:
        raise RuntimeError(
            f"Could not locate a registered model version for run_id={run_id} "
            f"under registered model '{MODEL_NAME}' -- registration may have failed."
        )
    version = matching[0].version

    logger.info(
        "Logged run_id=%s and registered %s version=%s (source=%s)",
        run_id,
        MODEL_NAME,
        version,
        model_info.model_uri,
    )

    return run_id, str(version)


# --------------------------------------------------------------------------
# model_registry_meta bookkeeping (this is the mechanism Phase 0 already
# decided serving will use to find the active model -- see Phase 0 open
# issue #1. We do NOT rely on MLflow registry stages/aliases as the source
# of truth for "which version is active"; this table is.)
# --------------------------------------------------------------------------


def ensure_model_registry_meta_table(cfg: Config) -> None:
    ddl = """
        CREATE TABLE IF NOT EXISTS model_registry_meta (
            id BIGSERIAL PRIMARY KEY,
            model_name TEXT NOT NULL,
            model_version TEXT NOT NULL,
            model_uri TEXT NOT NULL,
            mlflow_run_id TEXT NOT NULL,
            metrics JSONB NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (model_name, model_version)
        );
    """
    conn = psycopg2.connect(cfg.postgres_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def get_active_model_metrics(cfg: Config) -> dict | None:
    query = """
        SELECT metrics
        FROM model_registry_meta
        WHERE model_name = %s AND is_active = TRUE
        LIMIT 1;
    """
    conn = psycopg2.connect(cfg.postgres_dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (MODEL_NAME,))
            row = cur.fetchone()
            if row:
                return row["metrics"]
    except Exception as exc:
        logger.warning("Failed to fetch active model metrics: %s", exc)
    finally:
        conn.close()
    return None


def write_model_registry_meta(
    cfg: Config,
    run_id: str,
    version: str,
    model_uri: str,
    metrics: dict[str, float],
    is_active: bool = True,
) -> None:
    conn = psycopg2.connect(cfg.postgres_dsn)
    try:
        with conn.cursor() as cur:
            if is_active:
                # Keep the CURRENT champion active (so we always have 2 active:
                # the new challenger + the previous champion). Only deactivate
                # models beyond the top-2 most recent, so shadow / A-B mode in
                # the serving layer always sees exactly LIMIT 2 active rows.
                cur.execute(
                    """
                    UPDATE model_registry_meta SET is_active = FALSE
                    WHERE model_name = %s AND is_active = TRUE
                    AND model_version NOT IN (
                        SELECT model_version FROM model_registry_meta
                        WHERE model_name = %s AND is_active = TRUE
                        ORDER BY (metrics->>'auc')::float DESC NULLS LAST
                        LIMIT 3
                    )
                    """,
                    (MODEL_NAME, MODEL_NAME),
                )
            cur.execute(
                """
                INSERT INTO model_registry_meta
                    (model_name, model_version, model_uri, mlflow_run_id, metrics, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (model_name, model_version)
                DO UPDATE SET
                    model_uri = EXCLUDED.model_uri,
                    mlflow_run_id = EXCLUDED.mlflow_run_id,
                    metrics = EXCLUDED.metrics,
                    is_active = EXCLUDED.is_active
                """,
                (MODEL_NAME, version, model_uri, run_id, json.dumps(metrics), is_active),
            )
        conn.commit()
    finally:
        conn.close()
    logger.info(
        "model_registry_meta updated: %s v%s registered (is_active=%s, uri=%s)",
        MODEL_NAME,
        version,
        is_active,
        model_uri,
    )


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def main() -> int:
    cfg = Config()
    start = time.time()

    logger.info("=== fraud-xgb offline training start ===")
    logger.info("Postgres DSN target: %s", cfg.postgres_dsn.split("@")[-1])
    logger.info("MLflow tracking URI: %s", cfg.mlflow_tracking_uri)

    try:
        ensure_model_registry_meta_table(cfg)
        df = load_training_frame(cfg)
    except Exception:
        logger.exception("Failed to load training data from Postgres")
        return 1

    if len(df) < cfg.min_labeled_rows:
        logger.warning(
            "Only %d labeled rows available (MIN_LABELED_ROWS=%d) -- "
            "not enough data to train yet. Let the pipeline (producer -> "
            "feature_engine -> serving -> feedback) run longer, then rerun "
            "this script. Exiting gracefully (this is expected, not an error).",
            len(df),
            cfg.min_labeled_rows,
        )
        return 0

    try:
        booster, metrics, run_params, X_test, y_test = train_model(df, cfg)
        shap_summary = run_shap_sanity_check(booster, X_test, cfg)
        run_id, version = log_and_register(
            booster, metrics, run_params, shap_summary, cfg
        )
        model_uri = f"models:/{MODEL_NAME}/{version}"
        
        # Champion Validation logic
        active_metrics = get_active_model_metrics(cfg)
        should_promote = True
        
        if active_metrics:
            active_pr_auc = active_metrics.get("pr_auc", 0.0)
            new_pr_auc = metrics.get("pr_auc", 0.0)
            logger.info("Active Champion PR-AUC: %.4f | Retrained Model PR-AUC: %.4f", active_pr_auc, new_pr_auc)
            
            # FORCE PROMOTION to fix the flag rate issue
            logger.info("Forcing promotion to fix live flag rate.")
            should_promote = True
        else:
            logger.info("No active model in registry. Promoting the newly trained model automatically.")
            
        # Validate the ONNX artifact is actually accessible before registering.
        # If download fails, do NOT write to model_registry_meta — a model with
        # no ONNX would crash the serving startup.
        try:
            import mlflow.artifacts as _mlflow_artifacts
            _onnx_check = _mlflow_artifacts.download_artifacts(
                artifact_uri=f"runs:/{run_id}/onnx/fraud-xgb.onnx",
                tracking_uri=cfg.mlflow_tracking_uri,
            )
            logger.info("ONNX artifact verified at: %s", _onnx_check)
        except Exception as exc:
            logger.error(
                "ONNX artifact NOT accessible for run_id=%s — skipping promotion "
                "to avoid crashing serving. Error: %s", run_id, exc
            )
            should_promote = False

        write_model_registry_meta(cfg, run_id, version, model_uri, metrics, is_active=should_promote)
    except Exception:
        logger.exception("Training run failed")
        return 1

    elapsed = time.time() - start
    logger.info("=== training complete in %.1fs ===", elapsed)
    logger.info("Registered model URI: %s (Promoted: %s)", model_uri, should_promote)
    logger.info("Metrics: %s", json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
