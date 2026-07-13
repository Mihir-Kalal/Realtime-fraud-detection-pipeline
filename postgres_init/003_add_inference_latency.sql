-- Migration: Add inference_latency_ms to predictions table.
-- This column stores the ONNX inference time only (excluding Redis fetch),
-- giving visibility into sub-ms latency variance.
-- Nullable so existing rows are unaffected (backward compatible).

ALTER TABLE predictions
    ADD COLUMN IF NOT EXISTS inference_latency_ms DOUBLE PRECISION;
