"""
monitoring/drift.py

Detects feature/prediction drift and triggers retraining when a threshold
is breached. Runs as a long-lived polling loop (same shape as feedback/).

METHOD
------
- **Feature drift:** Population Stability Index (PSI) per locked
  FEATURE_COLUMN (identical list/order to training/train.py and
  serving/config.py — imported from monitoring/feature_columns.py, the
  single source of truth so this list can never silently diverge across
  Phase 3/4/5), comparing a *live* window of `feature_snapshots` (Postgres,
  Phase 2's offline table) against a *baseline* distribution established
  once per active model version.
- **Prediction drift:** two-sample Kolmogorov-Smirnov test comparing the
  live `predictions.fraud_probability` distribution against the baseline
  probability distribution captured at the same time the feature baseline
  was established.

WHY POSTGRES `feature_snapshots`, NOT REDIS
---------------------------------------------
Same reasoning as Phase 3's training data source: Redis feature hashes
TTL out (48h, per Phase 2) and only hold the *current* value per user, not
a historical population. `feature_snapshots` is the durable offline record
of what features actually looked like at scoring time, so it's the correct
source for building both the "baseline" (older window) and "live" (recent
window) distributions.

BASELINE ESTABLISHMENT (first run per model version)
------------------------------------------------------
`train.py` (Phase 3) does not currently persist a feature-distribution
baseline artifact — that's new surface added by this phase, not a redefinition of Phase 3's contract:
On the first drift check after a given `model_registry_meta.model_version`
becomes active, `establish_baseline()` computes decile bucket edges from
the `BASELINE_WINDOW_HOURS` of `feature_snapshots` immediately preceding
that model's `created_at`, and persists them to `drift_baseline_stats`
(new table, created idempotently here). All subsequent checks against that
model version reuse the stored baseline — this is intentional: comparing
"live" to "the exact training distribution", not to a constantly-sliding
recent window, is what makes PSI meaningful.

THRESHOLDS
----------
- PSI < 0.10            -> no significant drift
- 0.10 <= PSI < 0.25     -> moderate drift (logged, not actioned alone)
- PSI >= 0.25 (per feature) OR mean PSI >= 0.25 -> drift, retrain triggered
  (DRIFT_PSI_THRESHOLD env-overridable; these are the standard industry
  PSI cutoffs, not arbitrarily chosen).
- KS test: prediction-distribution drift alone (p < 0.01, i.e.
  KS_PVALUE_THRESHOLD) is logged as a signal but, by itself, does NOT
  trigger retraining — prediction distributions legitimately shift with
  fraud rings' behavior even when the *feature* distributions the model
  was trained on are still valid. It's surfaced in DriftCheckResult /
  drift_events for a human or a stricter downstream policy to act on.

RETRAIN TRIGGER — EXACT MECHANISM
-----------------------------------
Reuses training/train.py EXACTLY as-is (same feature schema via the shared
monitoring/feature_columns.py <-> training's FEATURE_COLUMNS, same MLflow
registration flow, same `model_registry_meta` bookkeeping) by invoking it
as a subprocess: `python -m training.train`. No parallel/alternative
training code path is introduced. A cooldown (DRIFT_RETRAIN_COOLDOWN_MINUTES,
default 60) prevents retrain storms if PSI stays elevated across
consecutive checks.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np
from scipy import stats

from common.db import get_conn, dict_cursor
from common.feature_columns import FEATURE_COLUMNS

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s monitoring.drift: %(message)s",
)
logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = float(os.environ.get("DRIFT_CHECK_INTERVAL_SECONDS", "300"))
LIVE_WINDOW_HOURS = float(os.environ.get("DRIFT_LIVE_WINDOW_HOURS", "6"))
BASELINE_WINDOW_HOURS = float(os.environ.get("DRIFT_BASELINE_WINDOW_HOURS", "72"))
MIN_LIVE_ROWS = int(os.environ.get("DRIFT_MIN_LIVE_ROWS", "200"))
NUM_BUCKETS = int(os.environ.get("DRIFT_PSI_BUCKETS", "10"))
PSI_THRESHOLD = float(os.environ.get("DRIFT_PSI_THRESHOLD", "0.25"))
KS_PVALUE_THRESHOLD = float(os.environ.get("DRIFT_KS_PVALUE_THRESHOLD", "0.01"))
RETRAIN_COOLDOWN_MINUTES = float(os.environ.get("DRIFT_RETRAIN_COOLDOWN_MINUTES", "60"))
TRAIN_MODULE = os.environ.get("DRIFT_TRAIN_COMMAND", "training.train")
TRAIN_TIMEOUT_SECONDS = float(os.environ.get("DRIFT_TRAIN_TIMEOUT_SECONDS", "3600"))

EPS = 1e-6

DDL = """
CREATE TABLE IF NOT EXISTS drift_baseline_stats (
    id BIGSERIAL PRIMARY KEY,
    model_version TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    bucket_edges JSONB NOT NULL,
    bucket_probs JSONB NOT NULL,
    prediction_probs JSONB,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (model_version, feature_name)
);

CREATE TABLE IF NOT EXISTS drift_events (
    id BIGSERIAL PRIMARY KEY,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    model_version TEXT NOT NULL,
    feature_psi JSONB NOT NULL,
    max_feature_psi DOUBLE PRECISION NOT NULL,
    mean_feature_psi DOUBLE PRECISION NOT NULL,
    prediction_ks_statistic DOUBLE PRECISION,
    prediction_ks_pvalue DOUBLE PRECISION,
    drift_detected BOOLEAN NOT NULL,
    retrain_triggered BOOLEAN NOT NULL,
    retrain_skipped_reason TEXT,
    retrain_exit_code INTEGER,
    retrain_stdout_tail TEXT
);
"""

ACTIVE_MODEL_SQL = """
SELECT model_version, created_at
FROM model_registry_meta
WHERE model_name = 'fraud-xgb' AND is_active = TRUE
ORDER BY created_at DESC
LIMIT 1;
"""

BASELINE_FEATURES_SQL = f"""
SELECT {', '.join(FEATURE_COLUMNS)}
FROM feature_snapshots
WHERE computed_at >= %(start)s AND computed_at < %(end)s;
"""

LIVE_FEATURES_SQL = f"""
SELECT {', '.join(FEATURE_COLUMNS)}
FROM feature_snapshots
WHERE computed_at >= %(start)s;
"""

BASELINE_PREDICTIONS_SQL = """
SELECT fraud_probability FROM predictions
WHERE scored_at >= %(start)s AND scored_at < %(end)s;
"""

LIVE_PREDICTIONS_SQL = """
SELECT fraud_probability FROM predictions
WHERE scored_at >= %(start)s;
"""

LAST_RETRAIN_SQL = """
SELECT checked_at FROM drift_events
WHERE retrain_triggered = TRUE
ORDER BY checked_at DESC
LIMIT 1;
"""

INSERT_EVENT_SQL = """
INSERT INTO drift_events (
    model_version, feature_psi, max_feature_psi, mean_feature_psi,
    prediction_ks_statistic, prediction_ks_pvalue, drift_detected,
    retrain_triggered, retrain_skipped_reason, retrain_exit_code,
    retrain_stdout_tail
) VALUES (
    %(model_version)s, %(feature_psi)s, %(max_feature_psi)s, %(mean_feature_psi)s,
    %(prediction_ks_statistic)s, %(prediction_ks_pvalue)s, %(drift_detected)s,
    %(retrain_triggered)s, %(retrain_skipped_reason)s, %(retrain_exit_code)s,
    %(retrain_stdout_tail)s
);
"""


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)


def get_active_model(conn) -> dict | None:
    with dict_cursor(conn) as cur:
        cur.execute(ACTIVE_MODEL_SQL)
        return cur.fetchone()


def _quantile_edges(values: np.ndarray, num_buckets: int) -> np.ndarray:
    """Decile (or NUM_BUCKETS-ile) edges from the baseline sample, with the
    outer edges pinned to +/-inf so any live value — even outside the
    baseline's observed range — falls into a bucket instead of being
    silently dropped (an out-of-range value is itself a drift signal)."""
    qs = np.linspace(0, 1, num_buckets + 1)
    edges = np.quantile(values, qs)
    edges = np.unique(edges)
    if len(edges) < 2:
        # Degenerate (constant) baseline feature — force a 2-bucket split
        # around the single value so PSI is still computable.
        v = float(edges[0]) if len(edges) else 0.0
        edges = np.array([v - 1.0, v, v + 1.0])
    edges[0] = -np.inf
    edges[-1] = np.inf
    return edges


def _bucket_probs(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    counts, _ = np.histogram(values, bins=edges)
    total = counts.sum()
    if total == 0:
        return np.full(len(counts), EPS)
    probs = counts / total
    return np.clip(probs, EPS, None)


def psi(baseline_probs: np.ndarray, live_probs: np.ndarray) -> float:
    return float(np.sum((live_probs - baseline_probs) * np.log(live_probs / baseline_probs)))


def establish_baseline(conn, model_version: str, model_created_at: datetime) -> dict:
    """Computes and persists baseline PSI bucket edges/probs for every
    feature, plus a baseline prediction-probability sample, for this model
    version. No-op (just re-reads) if already computed."""
    with dict_cursor(conn) as cur:
        cur.execute(
            "SELECT feature_name, bucket_edges, bucket_probs, prediction_probs "
            "FROM drift_baseline_stats WHERE model_version = %(mv)s",
            {"mv": model_version},
        )
        existing = {r["feature_name"]: r for r in cur.fetchall()}

    if len(existing) == len(FEATURE_COLUMNS) and existing:
        logger.info("baseline already established for model_version=%s", model_version)
        pred_probs = existing[FEATURE_COLUMNS[0]]["prediction_probs"]
        return {
            "edges": {f: np.array(existing[f]["bucket_edges"]) for f in FEATURE_COLUMNS},
            "probs": {f: np.array(existing[f]["bucket_probs"]) for f in FEATURE_COLUMNS},
            "prediction_probs": np.array(pred_probs) if pred_probs else np.array([]),
        }

    window_end = model_created_at
    window_start = window_end - timedelta(hours=BASELINE_WINDOW_HOURS)

    with dict_cursor(conn) as cur:
        cur.execute(BASELINE_FEATURES_SQL, {"start": window_start, "end": window_end})
        feat_rows = cur.fetchall()
        cur.execute(BASELINE_PREDICTIONS_SQL, {"start": window_start, "end": window_end})
        pred_rows = cur.fetchall()

    if len(feat_rows) < MIN_LIVE_ROWS:
        raise ValueError(
            f"Only {len(feat_rows)} feature_snapshots rows in baseline window "
            f"[{window_start}, {window_end}) for model_version={model_version}; "
            f"need >= {MIN_LIVE_ROWS} to establish a baseline. Widen "
            f"DRIFT_BASELINE_WINDOW_HOURS or wait for more data."
        )

    arr = {f: np.array([float(r[f]) for r in feat_rows]) for f in FEATURE_COLUMNS}
    pred_arr = np.array([float(r["fraud_probability"]) for r in pred_rows]) if pred_rows else np.array([])

    edges_map, probs_map = {}, {}
    with conn.cursor() as cur:
        for f in FEATURE_COLUMNS:
            edges = _quantile_edges(arr[f], NUM_BUCKETS)
            probs = _bucket_probs(arr[f], edges)
            edges_map[f], probs_map[f] = edges, probs
            cur.execute(
                """
                INSERT INTO drift_baseline_stats
                    (model_version, feature_name, bucket_edges, bucket_probs, prediction_probs)
                VALUES (%(mv)s, %(fn)s, %(edges)s, %(probs)s, %(pp)s)
                ON CONFLICT (model_version, feature_name) DO NOTHING;
                """,
                {
                    "mv": model_version,
                    "fn": f,
                    "edges": json.dumps(_finite(edges.tolist())),
                    "probs": json.dumps(probs.tolist()),
                    "pp": json.dumps(pred_arr.tolist()),
                },
            )

    logger.info(
        "established baseline for model_version=%s from %d feature rows / %d prediction rows",
        model_version, len(feat_rows), len(pred_rows),
    )
    return {"edges": edges_map, "probs": probs_map, "prediction_probs": pred_arr}


def _finite(values: list[float]) -> list[float]:
    """JSON has no +/-inf; swap the pinned outer edges for large finite
    sentinels on write, restored to +/-inf on... actually not restored —
    only used as bucket boundaries via np.histogram which treats the
    sentinel as effectively unbounded for any real feature value we'll see.
    """
    big = 1e18
    return [(-big if v == -np.inf else (big if v == np.inf else v)) for v in values]


def check_drift(conn, model_version: str, baseline: dict) -> dict:
    live_start = datetime.now(timezone.utc) - timedelta(hours=LIVE_WINDOW_HOURS)

    with dict_cursor(conn) as cur:
        cur.execute(LIVE_FEATURES_SQL, {"start": live_start})
        feat_rows = cur.fetchall()
        cur.execute(LIVE_PREDICTIONS_SQL, {"start": live_start})
        pred_rows = cur.fetchall()

    if len(feat_rows) < MIN_LIVE_ROWS:
        raise ValueError(
            f"Only {len(feat_rows)} live feature_snapshots rows in the last "
            f"{LIVE_WINDOW_HOURS}h; need >= {MIN_LIVE_ROWS} for a statistically "
            f"meaningful PSI. Skipping this check."
        )

    feature_psi = {}
    for f in FEATURE_COLUMNS:
        live_vals = np.array([float(r[f]) for r in feat_rows])
        live_probs = _bucket_probs(live_vals, baseline["edges"][f])
        feature_psi[f] = round(psi(baseline["probs"][f], live_probs), 5)

    max_psi = max(feature_psi.values())
    mean_psi = float(np.mean(list(feature_psi.values())))

    ks_stat, ks_pvalue = None, None
    if pred_rows and len(baseline["prediction_probs"]) >= 20:
        live_pred = np.array([float(r["fraud_probability"]) for r in pred_rows])
        if len(live_pred) >= 20:
            result = stats.ks_2samp(baseline["prediction_probs"], live_pred)
            ks_stat, ks_pvalue = float(result.statistic), float(result.pvalue)

    drift_detected = max_psi >= PSI_THRESHOLD or mean_psi >= PSI_THRESHOLD

    return {
        "model_version": model_version,
        "feature_psi": feature_psi,
        "max_feature_psi": max_psi,
        "mean_feature_psi": mean_psi,
        "prediction_ks_statistic": ks_stat,
        "prediction_ks_pvalue": ks_pvalue,
        "drift_detected": drift_detected,
    }


def cooldown_active(conn) -> bool:
    with dict_cursor(conn) as cur:
        cur.execute(LAST_RETRAIN_SQL)
        row = cur.fetchone()
    if not row:
        return False
    elapsed = datetime.now(timezone.utc) - row["checked_at"]
    return elapsed < timedelta(minutes=RETRAIN_COOLDOWN_MINUTES)


def trigger_retrain() -> tuple[int, str]:
    """Invokes training/train.py EXACTLY as Phase 3 defined it — same
    module, no parameters overridden, same MLflow registration flow. This
    is the only retraining code path in the repo."""
    logger.warning("drift threshold breached -> triggering retrain via %s", TRAIN_MODULE)
    proc = subprocess.run(
        [sys.executable, "-m", TRAIN_MODULE],
        cwd=os.environ.get("REPO_ROOT", "."),
        capture_output=True,
        text=True,
        timeout=TRAIN_TIMEOUT_SECONDS,
    )
    tail = (proc.stdout[-2000:] + "\n" + proc.stderr[-2000:]).strip()
    if proc.returncode == 0:
        logger.info("retrain finished successfully")
    else:
        logger.error("retrain FAILED with exit code %d:\n%s", proc.returncode, tail)
    return proc.returncode, tail


def run_once() -> dict:
    with get_conn() as conn:
        ensure_schema(conn)
        active = get_active_model(conn)
        if not active:
            raise RuntimeError(
                "No active row in model_registry_meta for model_name='fraud-xgb' — "
                "run training/train.py at least once before starting drift monitoring."
            )
        model_version = active["model_version"]
        baseline = establish_baseline(conn, model_version, active["created_at"])
        result = check_drift(conn, model_version, baseline)

        retrain_triggered = False
        skipped_reason = None
        exit_code, stdout_tail = None, None

        if result["drift_detected"]:
            if cooldown_active(conn):
                skipped_reason = (
                    f"drift detected but retrain cooldown "
                    f"({RETRAIN_COOLDOWN_MINUTES}min) still active"
                )
                logger.warning(skipped_reason)
            else:
                exit_code, stdout_tail = trigger_retrain()
                retrain_triggered = exit_code == 0
                if exit_code != 0:
                    skipped_reason = f"retrain subprocess exited {exit_code}"

        with conn.cursor() as cur:
            cur.execute(
                INSERT_EVENT_SQL,
                {
                    "model_version": model_version,
                    "feature_psi": json.dumps(result["feature_psi"]),
                    "max_feature_psi": result["max_feature_psi"],
                    "mean_feature_psi": result["mean_feature_psi"],
                    "prediction_ks_statistic": result["prediction_ks_statistic"],
                    "prediction_ks_pvalue": result["prediction_ks_pvalue"],
                    "drift_detected": result["drift_detected"],
                    "retrain_triggered": retrain_triggered,
                    "retrain_skipped_reason": skipped_reason,
                    "retrain_exit_code": exit_code,
                    "retrain_stdout_tail": stdout_tail,
                },
            )

        result["retrain_triggered"] = retrain_triggered
        result["retrain_skipped_reason"] = skipped_reason
        return result


def main() -> None:
    logger.info(
        "monitoring.drift starting: interval=%ss live_window=%sh baseline_window=%sh "
        "psi_threshold=%s cooldown=%smin",
        CHECK_INTERVAL_SECONDS, LIVE_WINDOW_HOURS, BASELINE_WINDOW_HOURS,
        PSI_THRESHOLD, RETRAIN_COOLDOWN_MINUTES,
    )
    while True:
        try:
            result = run_once()
            logger.info(
                "drift check: model_version=%s max_psi=%.4f mean_psi=%.4f drift=%s retrain=%s",
                result["model_version"], result["max_feature_psi"], result["mean_feature_psi"],
                result["drift_detected"], result["retrain_triggered"],
            )
        except Exception:
            logger.exception("error during drift check, will retry next interval")
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
