"""
Utility script to convert a trained XGBoost model (Booster) to ONNX format.

Enables ultra-low latency inference using ONNX Runtime (ORT), dropping scoring latency 
down to microsecond levels suitable for HFT or high-throughput production ML systems.
"""

import argparse
import logging
import os
import sys
import numpy as np
import xgboost as xgb

try:
    import onnxmltools
    import onnxruntime as ort
    from onnxconverter_common.data_types import FloatTensorType
except ImportError:
    print(
        "Required libraries for ONNX conversion (onnxmltools, onnxruntime, onnxconverter-common) are missing. "
        "Install them via: pip install onnxmltools onnxruntime onnxconverter-common"
    )
    sys.exit(1)

# Ensure project root is in path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.feature_columns import FEATURE_COLUMNS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("export_onnx")


def convert_xgboost_to_onnx(
    model_path: str,
    output_path: str,
    num_features: int = len(FEATURE_COLUMNS),
) -> None:
    """Loads an XGBoost Booster, converts it to ONNX format, and validates correctness."""
    logger.info("Loading XGBoost booster from %s", model_path)
    
    # Load model. Handles both raw booster save and XGBClassifier saves.
    booster = xgb.Booster()
    booster.load_model(model_path)
    
    logger.info("Converting XGBoost Booster to ONNX format (opset=15)...")
    # Define input type: Float tensor of shape [batch_size, num_features]
    initial_types = [("input", FloatTensorType([None, num_features]))]
    
    # Perform conversion
    onnx_model = onnxmltools.convert_xgboost(
        booster,
        initial_types=initial_types,
        target_opset=15,
    )
    
    # Save the output file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    onnxmltools.utils.save_model(onnx_model, output_path)
    logger.info("Successfully exported ONNX model to %s", output_path)
    
    # Validate the ONNX model against raw Booster
    logger.info("Running validation checks between XGBoost and ONNX Runtime...")
    
    # Create test data (100 rows, matching number of features)
    np.random.seed(42)
    test_data = np.random.randn(100, num_features).astype(np.float32)
    
    # 1. XGBoost predictions
    dtrain = xgb.DMatrix(test_data, feature_names=FEATURE_COLUMNS)
    xgb_preds = booster.predict(dtrain)
    
    # 2. ONNX Runtime predictions
    session = ort.InferenceSession(output_path)
    input_name = session.get_inputs()[0].name
    
    # ONNX runtime returns a list of outputs; for xgboost booster, it returns probabilities
    ort_preds_raw = session.run(None, {input_name: test_data})
    
    # Parse the outputs (typically a list of probabilities/labels depending on conversion)
    # The output format for XGBoost converted by onnxmltools is usually a single float array for probabilities
    ort_preds = np.squeeze(ort_preds_raw[0])
    
    # Compare
    max_diff = np.max(np.abs(xgb_preds - ort_preds))
    logger.info("Validation completed. Maximum prediction difference: %e", max_diff)
    
    if max_diff < 1e-4:
        logger.info("ONNX validation PASSED. Model is ready for production.")
    else:
        logger.warning(
            "ONNX validation WARNING. Discrepancy is higher than expected. Difference: %f",
            max_diff,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert trained XGBoost model to ONNX")
    parser.add_argument(
        "--model-path",
        required=True,
        help="Path to the saved XGBoost model file (e.g. model.json or model.bin)",
    )
    parser.add_argument(
        "--output-path",
        default="serving/model.onnx",
        help="Path where the output ONNX model should be written (default: serving/model.onnx)",
    )
    args = parser.parse_args()
    
    convert_xgboost_to_onnx(args.model_path, args.output_path)
