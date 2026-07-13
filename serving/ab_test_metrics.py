"""
serving/ab_test_metrics.py — Phase 6 (A/B Testing Metrics)

A simple script to compare precision, recall, and latency between the two
live model versions being A/B tested. Joins the predictions table (written
by Phase 6's serving API) against the labels table (written by Phase 5's
feedback loop).
"""
import sys
import os
import argparse
from typing import Dict, Any

# Ensure we can import from common
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.db import get_conn, dict_cursor

def compare_models(flag_threshold: float = 0.5) -> None:
    query = """
    WITH raw_metrics AS (
        SELECT 
            p.model_version,
            COUNT(*) as total_predictions,
            AVG(p.latency_ms) as avg_latency_ms,
            SUM(CASE WHEN p.is_flagged THEN 1 ELSE 0 END) as predicted_positives,
            SUM(CASE WHEN l.is_fraud = TRUE THEN 1 ELSE 0 END) as actual_positives,
            SUM(CASE WHEN p.is_flagged AND l.is_fraud = TRUE THEN 1 ELSE 0 END) as true_positives
        FROM predictions p
        JOIN labels l ON p.txn_id = l.txn_id
        WHERE p.scored_at >= now() - interval '24 hours'
        GROUP BY p.model_version
    )
    SELECT
        model_version,
        total_predictions,
        avg_latency_ms,
        predicted_positives,
        actual_positives,
        true_positives,
        CASE WHEN predicted_positives > 0 THEN true_positives::FLOAT / predicted_positives ELSE 0.0 END as precision,
        CASE WHEN actual_positives > 0 THEN true_positives::FLOAT / actual_positives ELSE 0.0 END as recall
    FROM raw_metrics
    ORDER BY model_version DESC;
    """
    try:
        with get_conn() as conn:
            with dict_cursor(conn) as cur:
                cur.execute(query)
                results = cur.fetchall()
        
        if not results:
            print("No prediction/label joins found. Ensure the feedback loop has run and labels match scored txns.")
            return

        print("=== A/B TEST METRICS COMPARISON ===")
        print(f"Threshold: {flag_threshold}")
        print("-" * 80)
        print(f"{'Model Version':<15} | {'Count':<8} | {'Precision':<10} | {'Recall':<10} | {'Avg Latency (ms)':<15}")
        print("-" * 80)
        for row in results:
            print(f"{row['model_version']:<15} | {row['total_predictions']:<8} | {row['precision']:<10.4f} | {row['recall']:<10.4f} | {row['avg_latency_ms']:<15.2f}")
        print("-" * 80)

    except Exception as exc:
        print(f"Failed to query metrics: {exc}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare A/B test model metrics.")
    parser.add_argument("--threshold", type=float, default=0.5, help="Fraud probability threshold for positive flag.")
    args = parser.parse_args()
    compare_models(args.threshold)
