-- Phase 2: offline feature snapshot table, written by feature_engine on
-- every processed transaction. Additive only — does not modify any Phase 0
-- locked table (transactions, predictions, labels, model_registry_meta).
-- feature_engine also creates this table idempotently at startup
-- (CREATE TABLE IF NOT EXISTS) so this file is redundant-but-safe if the
-- postgres_init/ directory is mounted fresh; kept here so the schema is
-- visible/reviewable without reading application code.

CREATE TABLE IF NOT EXISTS feature_snapshots (
    id                      BIGSERIAL PRIMARY KEY,
    txn_id                  TEXT NOT NULL UNIQUE,
    user_id                 TEXT NOT NULL,
    txn_timestamp           TIMESTAMPTZ NOT NULL,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    txn_velocity_1h         INTEGER NOT NULL,
    txn_velocity_24h        INTEGER NOT NULL,
    amount_mean_24h         DOUBLE PRECISION NOT NULL,
    amount_std_24h          DOUBLE PRECISION NOT NULL,
    amount_zscore           DOUBLE PRECISION NOT NULL,
    distinct_merchants_1h   INTEGER NOT NULL,
    distinct_merchants_24h  INTEGER NOT NULL,
    impossible_travel_flag  SMALLINT NOT NULL,
    seconds_since_last_txn  DOUBLE PRECISION NOT NULL,
    shared_device_count     INTEGER NOT NULL DEFAULT 0,
    shared_merchant_fraud_count INTEGER NOT NULL DEFAULT 0,
    hop_distance_to_fraud   INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_feature_snapshots_user_id
    ON feature_snapshots (user_id);
CREATE INDEX IF NOT EXISTS idx_feature_snapshots_txn_timestamp
    ON feature_snapshots (txn_timestamp);
