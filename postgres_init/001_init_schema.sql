-- Locked Postgres schema for fraud-pipeline (Final Unified Pipeline).
-- Tables: transactions, predictions, labels, model_registry_meta, 
--         feature_snapshots, drift_baseline_stats, drift_events
-- This script is mounted into the postgres container's
-- /docker-entrypoint-initdb.d/ and runs once on first container start.

CREATE TABLE IF NOT EXISTS transactions (
    txn_id              TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    currency            CHAR(3) NOT NULL,
    merchant_id         TEXT NOT NULL,
    merchant_category   TEXT NOT NULL,
    txn_timestamp       TIMESTAMPTZ NOT NULL,
    device_id           TEXT NOT NULL,
    ip_country          CHAR(2) NOT NULL,
    channel             TEXT NOT NULL CHECK (
                            channel IN ('card_present', 'card_not_present', 'upi', 'login')
                        ),
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions (user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_txn_timestamp ON transactions (txn_timestamp);

CREATE TABLE IF NOT EXISTS predictions (
    id                  BIGSERIAL PRIMARY KEY,
    txn_id              TEXT NOT NULL REFERENCES transactions (txn_id),
    fraud_probability   DOUBLE PRECISION NOT NULL CHECK (
                            fraud_probability >= 0 AND fraud_probability <= 1
                        ),
    is_flagged          BOOLEAN NOT NULL,
    model_version       TEXT NOT NULL,
    top_shap_features   JSONB NOT NULL,
    latency_ms          DOUBLE PRECISION NOT NULL CHECK (latency_ms >= 0),
    scored_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    label_confirmed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_predictions_txn_id ON predictions (txn_id);
CREATE INDEX IF NOT EXISTS idx_predictions_model_version ON predictions (model_version);

CREATE TABLE IF NOT EXISTS labels (
    id                  BIGSERIAL PRIMARY KEY,
    txn_id              TEXT NOT NULL UNIQUE REFERENCES transactions (txn_id),
    is_fraud            BOOLEAN NOT NULL,
    label_source        TEXT NOT NULL DEFAULT 'simulated_chargeback',
    confirmed_at        TIMESTAMPTZ NOT NULL,
    delay_hours         DOUBLE PRECISION NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_labels_confirmed_at ON labels (confirmed_at);

CREATE TABLE IF NOT EXISTS model_registry_meta (
    id BIGSERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    model_uri TEXT NOT NULL,
    mlflow_run_id TEXT NOT NULL,
    metrics JSONB NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (model_name, model_version)
);

CREATE INDEX IF NOT EXISTS idx_model_registry_active ON model_registry_meta (is_active);

-- Offline feature snapshot table (from Phase 2)
CREATE TABLE IF NOT EXISTS feature_snapshots (
    id                      BIGSERIAL PRIMARY KEY,
    txn_id                  TEXT NOT NULL UNIQUE REFERENCES transactions (txn_id),
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

-- Drift baseline and events tables (from Phase 5)
CREATE TABLE IF NOT EXISTS drift_baseline_stats (
    id BIGSERIAL PRIMARY KEY,
    model_version TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    bucket_edges JSONB NOT NULL,
    bucket_probs JSONB NOT NULL,
    prediction_probs JSONB,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (model_version, feature_name)
);

CREATE TABLE IF NOT EXISTS drift_events (
    id BIGSERIAL PRIMARY KEY,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    model_version TEXT NOT NULL,
    feature_psi JSONB NOT NULL,
    max_feature_psi DOUBLE PRECISION NOT NULL,
    mean_feature_psi DOUBLE PRECISION NOT NULL,
    prediction_ks_statistic DOUBLE PRECISION,
    prediction_ks_pvalue DOUBLE PRECISION,
    drift_detected BOOLEAN NOT NULL,
    retrain_triggered BOOLEAN NOT NULL,
    retrain_skipped_reason TEXT,
    retrain_exit_code INTEGER,
    retrain_stdout_tail TEXT
);
