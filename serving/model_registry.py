"""
serving/model_registry.py — Phase 5 UPDATE

Phase 4 loaded the model/explainer once at startup and never looked again
(documented as Open Issue #4: "No hot-reload"). Phase 5 closes that issue.

EXACT HOT-RELOAD CONTRACT (this is the part Phase 4's code had to change)
----------------------------------------------------------------------
- **Mechanism chosen: polling, not restart-on-deploy.** A background
  asyncio task (started in `main.py`'s lifespan, see updated main.py) calls
  `ModelHandle.maybe_reload()` every `MODEL_RELOAD_POLL_SECONDS` (default
  30s, env-overridable). It re-runs the exact same
  `SELECT ... FROM model_registry_meta WHERE is_active = TRUE` query Phase
  4 used at startup. If the returned `model_version` differs from what's
  currently loaded, it loads the new model + rebuilds the SHAP explainer
  in the background, and only THEN atomically swaps the reference that
  `scoring.py` reads from — in-flight requests using the old booster
  finish against the old booster, never against a half-swapped state.
- **Why polling over restart-on-deploy:** restart-on-deploy would require
  an external orchestrator (k8s rollout, compose restart hook, etc.) to
  watch Postgres and restart the container — that's infrastructure this
  demo repo doesn't have and it reintroduces a dropped-connections blip on
  every model promotion. Polling keeps the reload contract entirely inside
  the service, works identically under plain `docker compose up` and any
  future orchestrator, and the whole point of `<100ms p99` serving is to
  avoid restarts on the hot path. The trade-off, made explicitly: a new
  model can take up to `MODEL_RELOAD_POLL_SECONDS` to take effect after
  `training/train.py` marks it active — acceptable for a fraud model
  (versus a security patch, say). Use the manual endpoint below if that
  latency needs to be zero for a specific promotion.
- **Manual override:** `POST /admin/reload` (added in main.py) forces an
  immediate out-of-band check, bypassing the poll interval, for operators
  who don't want to wait up to 30s after promoting a model. Protected by
  a shared-secret `ADMIN_TOKEN` env var (no token set -> endpoint disabled,
  returns 404, so it's never accidentally left open).
- **Failure handling:** if the new model_uri fails to load (bad artifact,
  MLflow unreachable), the reload attempt is logged and the OLD model
  keeps serving — a bad promotion never takes down `/score`. The failure
  is visible via `GET /health`'s new `last_reload_error` field.
- **What does NOT change:** the `/score` request/response contract,
  `FEATURE_COLUMNS` order, `model_registry_meta` schema, and the
  `mlflow.xgboost.load_model` + `shap.TreeExplainer(booster)` loading
  method are all unchanged from Phase 3/4 — only WHEN loading happens is
  new.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Any

import numpy as np

import mlflow.xgboost
import psycopg2
import psycopg2.extras
import shap

logger = logging.getLogger("serving.model_registry")

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://frauduser:fraudpass@localhost:5432/frauddb"
)
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MODEL_NAME", "fraud-xgb")
MODEL_RELOAD_POLL_SECONDS = float(os.environ.get("MODEL_RELOAD_POLL_SECONDS", "30"))

ACTIVE_MODEL_SQL = """
SELECT model_uri, model_version, mlflow_run_id, metrics
FROM model_registry_meta
WHERE model_name = %(model_name)s AND is_active = TRUE
ORDER BY (metrics->>'auc')::float DESC NULLS LAST
LIMIT 2;
"""

PERFORMANCE_SQL = """
SELECT
    p.model_version,
    SUM(CASE WHEN p.is_flagged AND l.is_fraud THEN 1 ELSE 0 END) AS tp,
    SUM(CASE WHEN p.is_flagged AND NOT l.is_fraud THEN 1 ELSE 0 END) AS fp,
    SUM(CASE WHEN NOT p.is_flagged AND l.is_fraud THEN 1 ELSE 0 END) AS fn
FROM predictions p
JOIN labels l ON p.txn_id = l.txn_id
WHERE p.model_version = ANY(%(versions)s)
GROUP BY p.model_version;
"""


class ModelLoadError(RuntimeError):
    pass


@dataclass
class LoadedModel:
    booster: Any
    explainer: "shap.TreeExplainer"
    ort_session: Any
    model_version: str
    model_uri: str
    mlflow_run_id: str
    loaded_at: float
    metrics: dict[str, Any]
    # Pre-allocated numpy buffer — reused every request to avoid per-call
    # malloc/GC pressure. Shape: (1, n_features). dtype: float32 matches
    # the ONNX FloatTensorType input. Access must be single-threaded per
    # request (each request fills it in and immediately calls ORT.run).
    input_buffer: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=np.float32))
    
    # Thompson Sampling / Multi-Armed Bandit parameters
    alpha: float = 1.0
    beta: float = 1.0

def _fetch_active_rows() -> list[dict]:
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(ACTIVE_MODEL_SQL, {"model_name": MODEL_NAME})
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        raise ModelLoadError(
            f"No active row in model_registry_meta for model_name={MODEL_NAME!r}. "
            f"Run training/train.py at least once first."
        )
    return rows


def _load_from_row(row: dict) -> LoadedModel:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = row["model_uri"]
    try:
        # Native loader (NOT mlflow.pyfunc) — required so shap.TreeExplainer
        # can see the real xgboost.Booster. Unchanged from Phase 3/4 contract.
        booster = mlflow.xgboost.load_model(model_uri)
        booster.set_param({"nthread": 1})
        
        # Load ONNX artifact with C++ engine tuned for minimum latency:
        # - intra_op_num_threads=1: no thread spawning overhead per inference
        # - ORT_SEQUENTIAL: no intra-op parallelism dispatch overhead
        # - ORT_ENABLE_ALL: full graph fusion / constant folding at load time
        import onnxruntime as ort
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = 1
        opts.inter_op_num_threads = 1
        opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        run_id = row["mlflow_run_id"]
        onnx_local_path = mlflow.artifacts.download_artifacts(
            artifact_uri=f"runs:/{run_id}/onnx/fraud-xgb.onnx",
            tracking_uri=MLFLOW_TRACKING_URI
        )
        ort_session = ort.InferenceSession(onnx_local_path, sess_options=opts)
    except Exception as exc:  # noqa: BLE001 - surfaced to caller with context
        raise ModelLoadError(f"Failed to load model from {model_uri}: {exc}") from exc

    explainer = shap.TreeExplainer(booster)
    # Determine feature count from the ONNX model input shape
    n_features = ort_session.get_inputs()[0].shape[1]
    input_buffer = np.zeros((1, n_features), dtype=np.float32)
    return LoadedModel(
        booster=booster,
        explainer=explainer,
        ort_session=ort_session,
        model_version=row["model_version"],
        model_uri=model_uri,
        mlflow_run_id=row["mlflow_run_id"],
        loaded_at=time.time(),
        input_buffer=input_buffer,
        metrics=row.get("metrics", {}),
    )


class ModelHandle:
    """Thread-safe holder for the currently-active model(s). `scoring.py`
    should call `handle.current_models` to read (never cache the booster/explainer
    across requests itself) so a swap mid-flight is picked up cleanly by
    the *next* request while in-flight requests keep their already-fetched
    reference."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current_models: list[LoadedModel] = []
        self.last_reload_error: Optional[str] = None
        self.last_reload_check_at: Optional[float] = None

    @property
    def current_models(self) -> list[LoadedModel]:
        with self._lock:
            if not self._current_models:
                raise ModelLoadError("Model not loaded yet")
            return list(self._current_models)

    @property
    def is_loaded(self) -> bool:
        with self._lock:
            return len(self._current_models) > 0

    def load_initial(self) -> None:
        rows = _fetch_active_rows()
        loaded = [_load_from_row(row) for row in rows]
        with self._lock:
            self._current_models = loaded
        for m in loaded:
            logger.info(
                "initial model loaded: version=%s uri=%s", m.model_version, m.model_uri
            )

    def maybe_reload(self, force: bool = False) -> bool:
        """Checks model_registry_meta for newer active versions and swaps
        in-place if found. Returns True if a swap happened. Never raises —
        failures are logged and recorded in `last_reload_error`, and the
        previously-loaded models (if any) keep serving."""
        self.last_reload_check_at = time.time()
        try:
            rows = _fetch_active_rows()
        except Exception as exc:  # noqa: BLE001
            self.last_reload_error = f"registry lookup failed: {exc}"
            logger.warning(self.last_reload_error)
            return False

        with self._lock:
            current_versions = {m.model_version: m for m in self._current_models}

        target_versions = [r["model_version"] for r in rows]
        
        if not force and set(target_versions) == set(current_versions.keys()):
            return False  # already serving the exact same active versions

        try:
            new_loaded = []
            for row in rows:
                v = row["model_version"]
                if v in current_versions:
                    new_loaded.append(current_versions[v])
                else:
                    new_loaded.append(_load_from_row(row))
        except Exception as exc:  # noqa: BLE001
            self.last_reload_error = str(exc)
            logger.error("model reload FAILED, keeping versions=%s: %s", list(current_versions.keys()), exc)
            return False

        with self._lock:
            self._current_models = new_loaded
            
        self.last_reload_error = None
        new_version_keys = [m.model_version for m in new_loaded]
        logger.info(
            "model HOT-RELOADED: %s -> %s",
            list(current_versions.keys()), new_version_keys,
        )
        return True

    def update_metrics(self) -> None:
        """Queries the DB for TP, FP, FN and updates alpha/beta for Thompson Sampling."""
        with self._lock:
            versions = [m.model_version for m in self._current_models]
            if not versions:
                return

        conn = psycopg2.connect(POSTGRES_DSN)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(PERFORMANCE_SQL, {"versions": versions})
                rows = cur.fetchall()
        except Exception as exc:
            logger.error("Failed to update metrics for MAB: %s", exc)
            return
        finally:
            conn.close()

        metrics_map = {row["model_version"]: row for row in rows}
        with self._lock:
            for m in self._current_models:
                row = metrics_map.get(m.model_version)
                if row:
                    tp = float(row["tp"] or 0)
                    fp = float(row["fp"] or 0)
                    fn = float(row["fn"] or 0)
                    m.alpha = tp + 1.0
                    m.beta = fp + fn + 1.0


# Module-level singleton, imported by main.py / scoring.py — same pattern
# Phase 4 used for "build once at startup", just now mutable-in-place.
model_handle = ModelHandle()
