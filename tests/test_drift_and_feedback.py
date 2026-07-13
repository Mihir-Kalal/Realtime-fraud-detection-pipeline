"""
Unit tests for Phase 5: drift PSI/bucket math (pure functions, no DB) and
the label simulator (pure function, no DB). No mocks of business logic —
these exercise the real numpy/scipy math and the real simulator function.
Does NOT require a live Postgres/MLflow — see tests/load_test_serving.py
(Phase 4) and manual `docker compose` runs for full-stack validation.

Run: PYTHONPATH=. pytest tests/test_drift_and_feedback.py -v
"""
import random

import numpy as np
import pytest

from monitoring.drift import _bucket_probs, _quantile_edges, psi
from feedback.simulate_labels import simulate_ground_truth, _home_country_for_user


def test_psi_near_zero_for_identical_distributions():
    rng = np.random.default_rng(1)
    baseline = rng.normal(0, 1, 5000)
    live = rng.normal(0, 1, 2000)
    edges = _quantile_edges(baseline, 10)
    base_probs = _bucket_probs(baseline, edges)
    live_probs = _bucket_probs(live, edges)
    assert psi(base_probs, live_probs) < 0.05


def test_psi_high_for_shifted_distribution():
    rng = np.random.default_rng(2)
    baseline = rng.normal(0, 1, 5000)
    shifted = rng.normal(2.5, 1, 2000)
    edges = _quantile_edges(baseline, 10)
    base_probs = _bucket_probs(baseline, edges)
    shifted_probs = _bucket_probs(shifted, edges)
    assert psi(base_probs, shifted_probs) > 0.25


def test_constant_feature_does_not_crash():
    const = np.array([7.0] * 500)
    edges = _quantile_edges(const, 10)
    probs = _bucket_probs(const, edges)
    assert abs(probs.sum() - 1.0) < 1e-3
    assert np.all(np.isfinite(probs))


def test_out_of_range_live_value_falls_into_edge_bucket():
    baseline = np.random.default_rng(3).normal(0, 1, 3000)
    edges = _quantile_edges(baseline, 10)
    live = np.array([1000.0, -1000.0, 0.0, 0.1])  # extreme outliers + normal
    probs = _bucket_probs(live, edges)
    assert abs(probs.sum() - 1.0) < 1e-3  # every value landed in some bucket


def test_home_country_is_deterministic_per_user():
    assert _home_country_for_user("user_42") == _home_country_for_user("user_42")


def test_simulate_ground_truth_base_rate_reasonable():
    rng = random.Random(7)
    low_risk_txn = {
        "user_id": "stable_user",
        "amount": 20.0,
        "ip_country": _home_country_for_user("stable_user"),
        "channel": "pos",
        "merchant_category": "grocery",
    }
    fraud_count = sum(simulate_ground_truth(low_risk_txn, rng) for _ in range(20000))
    rate = fraud_count / 20000
    # low-risk txn (home country, low amount, pos, grocery) should sit
    # close to BASE_FRAUD_RATE (0.02 default), well under 10%.
    assert rate < 0.10


def test_simulate_ground_truth_high_risk_more_likely_fraud():
    rng = random.Random(7)
    high_risk_txn = {
        "user_id": "risky_user",
        "amount": 6000.0,
        "ip_country": "XX",  # guaranteed to differ from computed home country
        "channel": "web",
        "merchant_category": "crypto",
    }
    low_risk_txn = {
        "user_id": "risky_user",
        "amount": 20.0,
        "ip_country": _home_country_for_user("risky_user"),
        "channel": "pos",
        "merchant_category": "grocery",
    }
    high_rate = sum(simulate_ground_truth(high_risk_txn, rng) for _ in range(5000)) / 5000
    low_rate = sum(simulate_ground_truth(low_risk_txn, rng) for _ in range(5000)) / 5000
    assert high_rate > low_rate


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
