"""
serving/main.py — Phase 5 UPDATE

Reconstructed per Phase 4's documented contract (the real Phase 4 file
wasn't visible in this chat — same caveat as Phase 4 re-emitting
common/schemas.py; diff against your actual serving/main.py and keep
whichever differs only in ways unrelated to hot-reload). The `/score`
request/response contract, error codes, and cold-start behavior are
UNCHANGED from Phase 4 — the only new surface is:
  1. A background polling task that hot-reloads the model (see
     model_registry.py docstring for the exact contract).
  2. `POST /admin/reload` for an immediate manual reload.
  3. `GET /health` gains `model_version`, `last_reload_check_at`, and
     `last_reload_error` fields.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse, ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware

from common.schemas import FraudScore, Transaction
from serving.db_async import db_pool
from serving.config import AB_SPLIT_PERCENTAGE, ASYNC_DB_WRITE, SCORING_MODE
from serving.circuit_breaker import redis_breaker
from serving.model_registry import ModelLoadError, model_handle, MODEL_RELOAD_POLL_SECONDS
from serving.redis_features import RedisFeatureStore
from serving.scoring import score_transaction, run_shap_explanation

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s serving.main: %(message)s",
)
logger = logging.getLogger("serving.main")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")  # unset -> /admin/reload disabled (404)
FLAG_THRESHOLD = float(os.environ.get("FLAG_THRESHOLD", "0.5"))

redis_store = RedisFeatureStore()
_reload_task: asyncio.Task | None = None

INSERT_PREDICTION_SQL = """
    INSERT INTO predictions (
        txn_id, fraud_probability, is_flagged, model_version,
        top_shap_features, latency_ms, inference_latency_ms
    )
    VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
"""


INSERT_TRANSACTION_SQL = """
    INSERT INTO transactions (
        txn_id, user_id, amount, currency, merchant_id,
        merchant_category, txn_timestamp, device_id, ip_country, channel
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (txn_id) DO NOTHING
"""


async def _write_prediction_to_db(txn: Transaction, score: FraudScore) -> None:
    """Write a FraudScore to the predictions table, ensuring the transaction exists first."""
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # 1. Ensure transaction exists in DB to satisfy foreign key constraint
                await conn.execute(INSERT_TRANSACTION_SQL,
                    txn.txn_id,
                    txn.user_id,
                    txn.amount,
                    txn.currency,
                    txn.merchant_id,
                    txn.merchant_category,
                    txn.timestamp,
                    txn.device_id,
                    txn.ip_country,
                    txn.channel.value if hasattr(txn.channel, 'value') else txn.channel,
                )
                # 2. Insert prediction
                await conn.execute(INSERT_PREDICTION_SQL,
                    score.txn_id,
                    score.fraud_probability,
                    score.is_flagged,
                    score.model_version,
                    json.dumps(score.top_shap_features),
                    score.latency_ms,
                    score.inference_latency_ms,
                )
    except Exception as exc:
        logger.error("Failed to log prediction for txn %s: %s", score.txn_id, exc)


async def log_prediction_background(txn: Transaction, score: FraudScore) -> None:
    """Called as a FastAPI BackgroundTask when ASYNC_DB_WRITE=true."""
    await _write_prediction_to_db(txn, score)


async def _update_shap_in_db(txn_id: str, shap_features: list[dict]) -> None:
    """Backfills the top_shap_features column for a prediction already written to DB."""
    import json as _json
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE predictions SET top_shap_features = $1::jsonb WHERE txn_id = $2",
                _json.dumps(shap_features), txn_id,
            )
    except Exception as exc:
        logger.error("Failed to update SHAP for txn %s: %s", txn_id, exc)


async def _compute_and_store_shap(
    txn_id: str,
    vector: list[float],
    model_snapshot,
    is_flagged: bool,
) -> None:
    """Background task: run SHAP explanation (CPU-bound, thread-pooled) then
    update the already-committed prediction row. Only runs for flagged transactions."""
    if not is_flagged:
        return
    try:
        shap_features = await run_shap_explanation(txn_id, vector, model_snapshot)
        await _update_shap_in_db(txn_id, shap_features)
    except Exception as exc:
        logger.warning("Background SHAP failed for txn %s: %s", txn_id, exc)


async def _reload_poll_loop() -> None:
    """Background task: re-checks model_registry_meta every
    MODEL_RELOAD_POLL_SECONDS and hot-swaps the model if a new version was
    promoted. Runs in-process alongside request handling — reload work
    itself (MLflow artifact fetch + SHAP explainer build) happens
    off the event loop via a thread so it never blocks in-flight /score
    requests."""
    while True:
        await asyncio.sleep(MODEL_RELOAD_POLL_SECONDS)
        try:
            await asyncio.to_thread(model_handle.maybe_reload)
            await asyncio.to_thread(model_handle.update_metrics)
        except Exception:
            logger.exception("unexpected error in reload poll loop")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _reload_task
    # Build model + explainer ONCE at startup (unchanged from Phase 4),
    # then hand off to the background poller for everything after.
    try:
        model_handle.load_initial()
        model_handle.update_metrics()
    except ModelLoadError as exc:
        logger.error("startup model load failed: %s", exc)
        raise

    await db_pool.connect()
    await redis_store.connect()
    _reload_task = asyncio.create_task(_reload_poll_loop())
    logger.info(
        "fraud-scoring-api started, models=%s, reload_poll=%ss",
        [m.model_version for m in model_handle.current_models], MODEL_RELOAD_POLL_SECONDS,
    )
    yield
    if _reload_task:
        _reload_task.cancel()
    await redis_store.close()
    await db_pool.close()


app = FastAPI(title="fraud-scoring-api", lifespan=lifespan, default_response_class=ORJSONResponse)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATS_SQL = {
    "overview": """
        SELECT
            COUNT(*) AS total_predictions,
            SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS total_flagged,
            ROUND(AVG(latency_ms)::numeric, 2) AS avg_latency_ms,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p95_latency_ms,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p99_latency_ms,
            ROUND(AVG(COALESCE(inference_latency_ms, latency_ms))::numeric, 3) AS avg_inference_ms,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY COALESCE(inference_latency_ms, latency_ms))::numeric, 3) AS p99_inference_ms,
            SUM(CASE WHEN latency_ms < 100 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 AS sla_compliance_pct
        FROM predictions
        WHERE scored_at >= now() - interval '1 hour'
    """,
    "fraud_rate_trend": """
        SELECT
            date_trunc('minute', scored_at) AS minute,
            COUNT(*) AS total,
            SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS flagged,
            ROUND(AVG(fraud_probability)::numeric, 4) AS avg_prob
        FROM predictions
        WHERE scored_at >= now() - interval '1 hour'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 60
    """,
    "traffic_split": """
        SELECT model_version, COUNT(*) AS count
        FROM (SELECT model_version FROM predictions ORDER BY scored_at DESC LIMIT 2000) sub
        GROUP BY model_version
    """,
    "latency_timeseries": """
        SELECT
            date_trunc('minute', scored_at) AS minute,
            ROUND(AVG(latency_ms)::numeric, 2) AS avg_ms,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p95_ms,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p99_ms,
            ROUND(AVG(COALESCE(inference_latency_ms, latency_ms))::numeric, 3) AS avg_inference_ms,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY COALESCE(inference_latency_ms, latency_ms))::numeric, 3) AS p99_inference_ms
        FROM predictions
        WHERE scored_at >= now() - interval '1 hour'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 60
    """,
    "drift_events": """
        SELECT checked_at, model_version, max_feature_psi, mean_feature_psi,
               drift_detected, retrain_triggered, retrain_skipped_reason
        FROM drift_events
        ORDER BY checked_at DESC
        LIMIT 10
    """,
    "ab_metrics": """
        SELECT
            p.model_version,
            COUNT(*) AS predictions,
            SUM(CASE WHEN l.txn_id IS NOT NULL AND p.is_flagged AND l.is_fraud THEN 1 ELSE 0 END)::FLOAT /
                NULLIF(SUM(CASE WHEN l.txn_id IS NOT NULL AND p.is_flagged THEN 1 ELSE 0 END), 0) AS precision,
            SUM(CASE WHEN l.txn_id IS NOT NULL AND p.is_flagged AND l.is_fraud THEN 1 ELSE 0 END)::FLOAT /
                NULLIF(SUM(CASE WHEN l.txn_id IS NOT NULL AND l.is_fraud THEN 1 ELSE 0 END), 0) AS recall,
            ROUND(AVG(p.latency_ms)::numeric, 2) AS avg_latency_ms,
            ROUND(AVG(COALESCE(p.inference_latency_ms, p.latency_ms))::numeric, 3) AS avg_inference_ms,
            SUM(CASE WHEN l.txn_id IS NULL THEN 1 ELSE 0 END) AS unlabelled_count
        FROM predictions p
        LEFT JOIN labels l ON p.txn_id = l.txn_id
        WHERE p.scored_at >= now() - interval '24 hours'
        GROUP BY p.model_version
    """,
    "recent_predictions": """
        SELECT p.txn_id, p.fraud_probability, p.is_flagged, p.model_version,
               p.latency_ms, p.scored_at, p.top_shap_features
        FROM predictions p
        ORDER BY p.scored_at DESC
        LIMIT 25
    """,
    "model_registry": """
        SELECT model_name, model_version, model_uri, metrics, is_active, created_at
        FROM model_registry_meta
        ORDER BY created_at DESC
        LIMIT 10
    """,
}


async def _run_stats_query(sql: str) -> list[dict]:
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning("stats query failed: %s", exc)
        return []


@app.get("/stats")
async def stats() -> JSONResponse:
    """Aggregate metrics for the management dashboard."""
    results = {}
    for key, sql in STATS_SQL.items():
        rows = await _run_stats_query(sql)
        # Coerce Decimal / datetime to JSON-safe types
        import decimal, datetime as dt
        def _serialize(obj):
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            if isinstance(obj, (dt.datetime, dt.date)):
                return obj.isoformat()
            return str(obj)
        import json as _json
        results[key] = _json.loads(_json.dumps(rows, default=_serialize))
    return JSONResponse(content=results)


@app.post("/score")
async def score(txn: Transaction, background_tasks: BackgroundTasks):
    if not model_handle.is_loaded:
        raise HTTPException(status_code=503, detail="model not loaded")

    try:
        # Grab a consistent model snapshot for this request
        models = model_handle.current_models
        primary_score, shadow_score, vector = await score_transaction(
            txn,
            models,
            redis_store,
            flag_threshold=FLAG_THRESHOLD,
            ab_split_percentage=AB_SPLIT_PERCENTAGE,
            scoring_mode=SCORING_MODE,
        )

        # Pick the model that scored the primary (for SHAP background task)
        from serving.scoring import pick_model, ScoringMode
        if SCORING_MODE == ScoringMode.SHADOW and len(models) >= 2:
            primary_model = models[1]
        else:
            primary_model = pick_model(txn.txn_id, models, AB_SPLIT_PERCENTAGE)

        # Persist prediction(s) — async or sync based on config
        if ASYNC_DB_WRITE:
            background_tasks.add_task(log_prediction_background, txn, primary_score)
            if shadow_score:
                background_tasks.add_task(log_prediction_background, txn, shadow_score)
        else:
            await _write_prediction_to_db(txn, primary_score)
            if shadow_score:
                await _write_prediction_to_db(txn, shadow_score)

        # Schedule SHAP explanation off the hot path (runs after response is sent)
        background_tasks.add_task(
            _compute_and_store_shap,
            txn.txn_id,
            vector,
            primary_model,
            primary_score.is_flagged,
        )

        return primary_score.model_dump()
    except Exception as exc:  # noqa: BLE001
        logger.exception("scoring failed for txn_id=%s", txn.txn_id)
        raise HTTPException(status_code=500, detail=f"scoring failed: {exc}") from exc


@app.get("/health")
async def health() -> JSONResponse:
    redis_ok = await redis_store.ping()
    model_ok = model_handle.is_loaded
    status_code = 200 if (redis_ok and model_ok) else 503
    body = {
        "status": "ok" if status_code == 200 else "degraded",
        "redis_ok": redis_ok,
        "circuit_breaker_state": redis_breaker.current_state.name if hasattr(redis_breaker.current_state, "name") else str(redis_breaker.current_state),
        "model_ok": model_ok,
        "model_versions": [m.model_version for m in model_handle.current_models] if model_ok else [],
        "model_uris": [m.model_uri for m in model_handle.current_models] if model_ok else [],
        "last_reload_check_at": model_handle.last_reload_check_at,
        "last_reload_error": model_handle.last_reload_error,
    }
    return JSONResponse(status_code=status_code, content=body)


@app.post("/admin/reload")
async def admin_reload(request: Request, x_admin_token: str | None = Header(default=None)):
    """Manual out-of-band reload, bypassing the poll interval. Disabled
    (404, indistinguishable from a nonexistent route) unless ADMIN_TOKEN is
    set, so it's never accidentally exposed."""
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=404, detail="not found")
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="forbidden")

    before_versions = [m.model_version for m in model_handle.current_models] if model_handle.is_loaded else []
    swapped = await asyncio.to_thread(model_handle.maybe_reload, True)
    return {
        "swapped": swapped,
        "previous_model_versions": before_versions,
        "current_model_versions": [m.model_version for m in model_handle.current_models],
        "checked_at": time.time(),
    }
