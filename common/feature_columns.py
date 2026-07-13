import hashlib

FEATURE_COLUMNS = [
    "txn_velocity_1h",
    "txn_velocity_24h",
    "amount_mean_24h",
    "amount_std_24h",
    "amount_zscore",
    "distinct_merchants_1h",
    "distinct_merchants_24h",
    "impossible_travel_flag",
    "seconds_since_last_txn",
    "shared_device_count",
    "shared_merchant_fraud_count",
    "hop_distance_to_fraud"
]

def _home_country_for_user(user_id: str) -> str:
    """Deterministic pseudo 'home country' per user, derived from a hash.
    Ensures that data generation and label simulation match on the country logic.
    """
    countries = ["US", "GB", "IN", "DE", "BR", "NG", "SG", "AU"]
    idx = int(hashlib.sha256(user_id.encode()).hexdigest(), 16) % len(countries)
    return countries[idx]
