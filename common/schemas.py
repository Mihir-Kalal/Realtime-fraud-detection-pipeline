"""
Canonical schemas — LOCKED, do not redefine or fork.

Transaction / FraudScore are re-emitted verbatim from the Phase 0 locked
contract (see PROJECT_STATE.md Phase 4, Open Issue #1: if your real
Phase 0 common/schemas.py differs from this file at all, treat your
existing file as the source of truth and reconcile before deploying).

LabelRecord / DriftCheckResult are NEW in Phase 5 and additive only —
they do not change Transaction or FraudScore in any way.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field, field_validator


class Channel(str, Enum):
    CARD_PRESENT = "card_present"
    CARD_NOT_PRESENT = "card_not_present"
    UPI = "upi"
    LOGIN = "login"


class Transaction(BaseModel):
    txn_id: str
    user_id: str
    amount: float = Field(..., ge=0)
    currency: str
    merchant_id: str
    merchant_category: str
    timestamp: datetime
    device_id: str
    ip_country: str
    channel: Channel

    @field_validator("currency")
    @classmethod
    def currency_upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("ip_country")
    @classmethod
    def country_upper(cls, v: str) -> str:
        return v.upper()

    model_config = ConfigDict(use_enum_values=True)


class FraudScore(BaseModel):
    txn_id: str
    fraud_probability: float = Field(..., ge=0.0, le=1.0)
    is_flagged: bool
    model_version: str
    top_shap_features: list[dict]
    latency_ms: float = Field(..., ge=0)
    inference_latency_ms: Optional[float] = Field(
        default=None, ge=0,
        description="ONNX inference time only (excludes Redis fetch). "
        "Added in Phase 5 fix for sub-ms latency visibility.",
    )

    model_config = ConfigDict(protected_namespaces=())


TRANSACTIONS_STREAM = "transactions:raw"
FEATURE_KEY_PATTERN = "features:user:{user_id}"


def feature_key(user_id: str) -> str:
    """Build the Redis feature-store key for a given user_id."""
    return FEATURE_KEY_PATTERN.format(user_id=user_id)



# ---------------------------------------------------------------------------
# Phase 5 additions (additive only — do not touch the two models above)
# ---------------------------------------------------------------------------

class LabelRecord(BaseModel):
    """Ground-truth label written by feedback/simulate_labels.py into the
    `labels` table. Column name for the boolean is `is_fraud` (see Phase 3
    Open Issue #1 — confirmed as `is_fraud` for this phase; update here and
    in feedback/db.py::LABEL_COLUMN if your real schema differs)."""

    txn_id: str
    is_fraud: bool
    label_source: str = Field(
        default="simulated_chargeback",
        description="Provenance of the label, e.g. simulated_chargeback, "
        "manual_review, issuer_chargeback_feed.",
    )
    confirmed_at: datetime
    delay_hours: float


class DriftCheckResult(BaseModel):
    """One row of monitoring/drift.py's output, also persisted to the
    `drift_events` table."""

    checked_at: datetime
    model_version: str
    feature_psi: dict[str, float]
    max_feature_psi: float
    mean_feature_psi: float
    prediction_ks_statistic: Optional[float] = None
    prediction_ks_pvalue: Optional[float] = None
    drift_detected: bool
    retrain_triggered: bool
    retrain_skipped_reason: Optional[str] = None

    model_config = ConfigDict(protected_namespaces=())
