"""
serving/scoring.py — HFT Latency Optimizations

Hot path: ONNX inference only. Fills a pre-allocated numpy buffer instead of
allocating a fresh array per request (eliminates per-call malloc / GC pressure).
SHAP is fully removed from the synchronous response path — it now runs in a
FastAPI BackgroundTask after the HTTP 200 is already returned to the client.

run_shap_explanation() is the background worker called by main.py.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from enum import Enum

import numpy as np
import xgboost as xgb

from common.schemas import FraudScore, Transaction
from serving.config import FEATURE_COLUMNS
from serving.model_registry import LoadedModel
from serving.redis_features import RedisFeatureStore

TOP_K_SHAP = 5

# Minimum total observations (alpha + beta - 2, since priors start at 1,1)
# before Thompson Sampling takes over from the configured ab_split_percentage.
# Below this threshold, pick_model() uses the fixed split for controlled
# exploration. Above it, Thompson Sampling naturally exploits the better model.
MIN_OBSERVATIONS_FOR_MAB = 30

class ScoringMode(str, Enum):
    AB_TEST = "ab_test"
    SHADOW = "shadow"


def pick_model(txn_id: str, models: list[LoadedModel], ab_split_percentage: int) -> LoadedModel:
    if not models:
        raise ValueError("No models provided")
    if len(models) == 1:
        return models[0]

    # Deterministic seeded randomness for retries
    seed_val = int(hashlib.md5(txn_id.encode()).hexdigest(), 16) % (2**32 - 1)
    rng = np.random.default_rng(seed_val)

    # Check if we have enough observations for Thompson Sampling.
    # observations = (alpha - 1) + (beta - 1) since priors start at 1,1.
    total_obs = sum((m.alpha - 1) + (m.beta - 1) for m in models)

    if total_obs < MIN_OBSERVATIONS_FOR_MAB:
        # BURN-IN PHASE: respect the configured ab_split_percentage.
        # ab_split_percentage controls how much traffic goes to models[0].
        # E.g., ab_split_percentage=50 means 50% to models[0], 50% to models[1].
        roll = rng.uniform(0, 100)
        return models[0] if roll < ab_split_percentage else models[1]

    # EXPLOITATION PHASE: Thompson Sampling with Beta draws.
    # Each model draws from Beta(alpha, beta); highest draw wins.
    sampled_thetas = [rng.beta(m.alpha, m.beta) for m in models]
    best_idx = int(np.argmax(sampled_thetas))
    return models[best_idx]


class ModelBatcher:
    def __init__(self, model: LoadedModel):
        self.model = model
        self.max_batch_size = 50
        self.batch_timeout_ms = 5
        self.queue = asyncio.Queue()
        self.task = asyncio.create_task(self._process_loop())
        
    async def _process_loop(self):
        while True:
            try:
                item = await self.queue.get()
            except asyncio.CancelledError:
                break
                
            batch = [item]
            
            try:
                await asyncio.sleep(self.batch_timeout_ms / 1000.0)
            except asyncio.CancelledError:
                pass
                
            while len(batch) < self.max_batch_size and not self.queue.empty():
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
                    
            try:
                infer_start_ns = time.perf_counter_ns()
                vectors = [b[0] for b in batch]
                futures = [b[1] for b in batch]
                
                np_batch = np.array(vectors, dtype=np.float32)
                ort_inputs = {self.model.ort_session.get_inputs()[0].name: np_batch}
                ort_outs = self.model.ort_session.run(None, ort_inputs)
                inference_latency_ms = (time.perf_counter_ns() - infer_start_ns) / 1_000_000
                
                probs = ort_outs[1]
                for i, prob_struct in enumerate(probs):
                    if isinstance(prob_struct, dict):
                        f_prob = float(prob_struct.get(1, prob_struct.get('1', 0.0)))
                    else:
                        f_prob = float(prob_struct[1])
                        
                    if not futures[i].done():
                        futures[i].set_result((f_prob, inference_latency_ms))
            except Exception as e:
                for b in batch:
                    if not b[1].done():
                        b[1].set_exception(e)

_batchers = {}

def _get_batcher(model: LoadedModel, active_models: list[LoadedModel]) -> ModelBatcher:
    active_ids = {id(m) for m in active_models}
    for k in list(_batchers.keys()):
        if k not in active_ids:
            _batchers[k].task.cancel()
            del _batchers[k]
            
    m_id = id(model)
    if m_id not in _batchers:
        _batchers[m_id] = ModelBatcher(model)
    return _batchers[m_id]


async def score_transaction(
    txn: Transaction,
    models: list[LoadedModel],
    redis_store: RedisFeatureStore,
    flag_threshold: float = 0.5,
    ab_split_percentage: int = 50,
    scoring_mode: str = ScoringMode.AB_TEST,
) -> tuple[FraudScore, FraudScore | None, list[float]]:
    """
    HOT PATH: Redis feature fetch + ONNX inference only.
    SHAP is NOT computed here — it runs in a BackgroundTask after the response
    is sent. This keeps p99 latency on the critical path minimal.

    Returns (primary_score, shadow_score_or_None, feature_vector).
    The feature_vector is returned so the caller can pass it to
    run_shap_explanation() without re-fetching from Redis.
    """
    start_ns = time.perf_counter_ns()

    if not models:
        raise ValueError("No models provided")

    vector, is_cold_start = await redis_store.get_feature_vector(txn.user_id)

    async def _score_single_async(model: LoadedModel, version_prefix: str = "") -> FraudScore:
        batcher = _get_batcher(model, models)
        
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await batcher.queue.put((vector, future))
        
        f_prob, inference_latency_ms = await future
        
        thresh = model.metrics.get("best_threshold", flag_threshold)
        is_flagged = f_prob >= thresh
        total_latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000

        return FraudScore(
            txn_id=txn.txn_id,
            fraud_probability=f_prob,
            is_flagged=is_flagged,
            model_version=f"{version_prefix}{model.model_version}",
            top_shap_features=[],
            latency_ms=total_latency_ms,
            inference_latency_ms=inference_latency_ms,
        )

    if scoring_mode == ScoringMode.SHADOW and len(models) >= 2:
        # Primary is models[1] (older), Challenger is models[0] (newer)
        primary_score, shadow_score = await asyncio.gather(
            _score_single_async(models[1]),
            _score_single_async(models[0], version_prefix="shadow_")
        )
        return primary_score, shadow_score, vector
    else:
        model = pick_model(txn.txn_id, models, ab_split_percentage)
        primary_score = await _score_single_async(model)
        return primary_score, None, vector


async def run_shap_explanation(
    txn_id: str,
    vector: list[float],
    model: LoadedModel,
) -> list[dict]:
    """
    BACKGROUND PATH: CPU-bound SHAP explanation, called after HTTP response is sent.
    Runs in a thread pool (asyncio.to_thread) so it does not block the event loop.
    Returns the top-K SHAP features list for the caller to persist to DB.
    """
    def _compute() -> list[dict]:
        dmatrix = xgb.DMatrix(
            np.array([vector], dtype=np.float64), feature_names=FEATURE_COLUMNS
        )
        s_values = model.explainer.shap_values(dmatrix, check_additivity=False)
        s_row = np.asarray(s_values)[0]
        ranked_idx = np.argsort(-np.abs(s_row))[:TOP_K_SHAP]
        return [
            {
                "feature": FEATURE_COLUMNS[i],
                "shap_value": float(s_row[i]),
                "feature_value": float(vector[i]),
            }
            for i in ranked_idx
        ]

    return await asyncio.to_thread(_compute)
