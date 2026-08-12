import pandera as pa
import pandas as pd
import dataclasses
from typing import Any

feature_schema = pa.DataFrameSchema({
    "txn_id": pa.Column(str),
    "user_id": pa.Column(str),
    "txn_velocity_1h": pa.Column(int, pa.Check.ge(1)),
    "txn_velocity_24h": pa.Column(int, pa.Check.ge(1)),
    "amount_mean_24h": pa.Column(float, pa.Check.ge(0.0)),
    "amount_std_24h": pa.Column(float, pa.Check.ge(0.0)),
    "amount_zscore": pa.Column(float),
    "distinct_merchants_1h": pa.Column(int, pa.Check.ge(1)),
    "distinct_merchants_24h": pa.Column(int, pa.Check.ge(1)),
    "impossible_travel_flag": pa.Column(int, pa.Check.isin([0, 1])),
    "seconds_since_last_txn": pa.Column(float, pa.Check.ge(-1.0)),
    "shared_device_count": pa.Column(int, pa.Check.ge(0)),
    "shared_merchant_fraud_count": pa.Column(int, pa.Check.ge(0)),
    "hop_distance_to_fraud": pa.Column(int, pa.Check.ge(0)),
    # Direct signal features aligned with label logic
    "is_foreign_ip": pa.Column(float, pa.Check.isin([0.0, 1.0])),
    "log_amount": pa.Column(float, pa.Check.ge(0.0)),
    "is_card_not_present": pa.Column(float, pa.Check.isin([0.0, 1.0])),
    "is_high_risk_category": pa.Column(float, pa.Check.isin([0.0, 1.0])),
})

def validate_feature_vector(fv: Any) -> None:
    """
    Validates a FeatureVector dataclass instance using Pandera.
    Raises pandera.errors.SchemaError if validation fails.
    """
    df = pd.DataFrame([dataclasses.asdict(fv)])
    feature_schema.validate(df)
