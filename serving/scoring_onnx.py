"""
serving/scoring_onnx.py

Alternative scoring module leveraging ONNX Runtime (ORT) for high-performance transaction scoring.
Demonstrates the separation of the latency-critical scoring path (using ONNX) from the heavier 
explainability path (using SHAP), a standard design pattern in High-Frequency Trading (HFT) and MLE environments.
"""

from __future__ import annotations

import logging
import time
import numpy as np
import xgboost as xgb
import onnxruntime as ort

from common.schemas import FraudScore, Transaction
from serving.config import FEATURE_COLUMNS
from serving.redis_features import RedisFeatureStore
from serving.model_registry import LoadedModel

logger = logging.getLogger("serving.scoring_onnx")
TOP_K_SHAP = 5


class OnnxScoringService:
    """
    Scoring service using ONNX Runtime for inference.
    Supports running inference only (latency-focused) or inference + SHAP (hybrid).
    """

    def __init__(self, onnx_model_path: str) -> None:
        self.onnx_model_path = onnx_model_path
        logger.info("Initializing ONNX Runtime Session for %s", onnx_model_path)
        
        # Set session options for optimal single-thread performance (standard for HFT/serving containers)
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = 1
        opts.inter_op_num_threads = 1
        opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        
        self.session = ort.InferenceSession(onnx_model_path, sess_options=opts)
        self.input_name = self.session.get_inputs()[0].name

    def score_onnx(self, vector: list[float]) -> float:
        """Run ultra-low-latency ONNX Runtime inference."""
        # Convert vector to numpy float32 matching the FloatTensorType
        input_data = np.array([vector], dtype=np.float32)
        
        # Run inference
        raw_outputs = self.session.run(None, {self.input_name: input_data})
        
        # Parse output probability
        prob = float(np.squeeze(raw_outputs[0]))
        return prob


async def score_transaction_onnx(
    txn: Transaction,
    models: list[LoadedModel], # fallback for SHAP/XGBoost if hybrid mode is active
    onnx_service: OnnxScoringService | None,
    redis_store: RedisFeatureStore,
    flag_threshold: float = 0.5,
    ab_split_percentage: int = 50,
    explain_shap: bool = False, # Toggle for latency-critical vs auditable modes
) -> FraudScore:
    """
    Scores a transaction using ONNX Runtime.
    
    If explain_shap is False, this runs in pure HFT-mode (scoring only, p99 < 2ms).
    If explain_shap is True, it runs in hybrid-mode, scoring with ONNX and running
    SHAP against the native XGBoost model booster for explainability features.
    """
    start = time.perf_counter()

    # Pick version for book-keeping
    # models[0] is the active version
    model = models[0] if models else None
    model_version = model.model_version if model else "onnx_model"

    # Fetch features from low-latency Redis cache
    vector, is_cold_start = await redis_store.get_feature_vector(txn.user_id)

    # Core scoring logic
    if onnx_service:
        # Fast path: ONNX Runtime
        fraud_probability = onnx_service.score_onnx(vector)
    elif model:
        # Fallback to standard XGBoost Booster
        dmatrix = xgb.DMatrix(np.array([vector], dtype=np.float64), feature_names=FEATURE_COLUMNS)
        fraud_probability = float(model.booster.predict(dmatrix)[0])
    else:
        raise ValueError("No scoring model or ONNX session available.")

    # Explainability logic
    top_shap_features = []
    if explain_shap and model:
        # Heavy path: SHAP explanation (usually 10-50x slower than scoring)
        def _run_shap():
            dmatrix = xgb.DMatrix(np.array([vector], dtype=np.float64), feature_names=FEATURE_COLUMNS)
            s_values = model.explainer.shap_values(dmatrix, check_additivity=False)
            return np.asarray(s_values)[0]

        shap_row = _run_shap()
        ranked_idx = np.argsort(-np.abs(shap_row))[:TOP_K_SHAP]
        top_shap_features = [
            {
                "feature": FEATURE_COLUMNS[i],
                "shap_value": float(shap_row[i]),
                "feature_value": float(vector[i]),
            }
            for i in ranked_idx
        ]
    else:
        # HFT-Mode: return empty shap features to satisfy contract without the latency hit
        top_shap_features = [
            {"feature": "SHAP_BYPASSED_FOR_LATENCY", "shap_value": 0.0, "feature_value": 0.0}
        ]

    latency_ms = (time.perf_counter() - start) * 1000.0

    return FraudScore(
        txn_id=txn.txn_id,
        fraud_probability=fraud_probability,
        is_flagged=fraud_probability >= flag_threshold,
        model_version=model_version,
        top_shap_features=top_shap_features,
        latency_ms=latency_ms,
    )
