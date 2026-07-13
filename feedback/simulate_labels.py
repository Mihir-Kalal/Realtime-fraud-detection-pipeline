"""
feedback/simulate_labels.py

Simulates ground-truth labels arriving late (e.g. a chargeback confirmed
N hours after the original transaction). Runs as a long-lived polling loop
(same "poll on an interval" shape as producer/ and feature_engine/ from
earlier phases — no new architectural pattern introduced).

WHAT IT DOES
------------
1. Finds transactions in Postgres `transactions` that are older than
   LABEL_DELAY_MIN_HOURS and don't have a label yet.
2. For each, "confirms" a ground-truth label using a deterministic-but-
   randomized simulator (see `simulate_ground_truth`) — this stands in for
   a real issuer chargeback feed / manual fraud-review outcome, which this
   demo pipeline has no access to.
3. Writes the confirmed label into `labels` (idempotent — ON CONFLICT DO
   NOTHING keyed on txn_id) and updates `predictions.label_confirmed_at`
   if that row exists, so downstream consumers can join without re-deriving
   delay.

WHY delay is bounded, not instant
----------------------------------
Real fraud labels are famously delayed and incomplete — chargebacks can
take weeks. LABEL_DELAY_MIN_HOURS/LABEL_DELAY_MAX_HOURS bound a uniform
random delay to keep this demo runnable in real time while still exercising
the "late-arriving label" code path that training/monitoring depend on.

LABEL SOURCE OF TRUTH FOR THE SIMULATOR
----------------------------------------
Because this is a simulation (no real chargeback network), the "true"
fraud outcome is derived from a fixed, seeded synthetic rule so that reruns
are reproducible and so the label distribution is realistic (low base
rate, correlated with known high-risk signals already present on the
Transaction: high amount, non-home ip_country pattern via a hash-based
per-user home-country model, and channel). This is clearly NOT a real
fraud model — see `simulate_ground_truth` docstring.
"""
from __future__ import annotations

import hashlib
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

from common.db import get_conn, dict_cursor

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s feedback.simulate_labels: %(message)s",
)
logger = logging.getLogger(__name__)

LABEL_DELAY_MIN_HOURS = float(os.environ.get("LABEL_DELAY_MIN_HOURS", "1"))
LABEL_DELAY_MAX_HOURS = float(os.environ.get("LABEL_DELAY_MAX_HOURS", "72"))
POLL_INTERVAL_SECONDS = float(os.environ.get("FEEDBACK_POLL_INTERVAL_SECONDS", "30"))
BATCH_SIZE = int(os.environ.get("FEEDBACK_BATCH_SIZE", "500"))
BASE_FRAUD_RATE = float(os.environ.get("SIMULATED_BASE_FRAUD_RATE", "0.02"))
LABEL_COLUMN = os.environ.get("LABEL_COLUMN", "is_fraud")  # Phase 3 Open Issue #1

DDL_LABELS = """
CREATE TABLE IF NOT EXISTS labels (
    id BIGSERIAL PRIMARY KEY,
    txn_id TEXT NOT NULL UNIQUE,
    is_fraud BOOLEAN NOT NULL,
    label_source TEXT NOT NULL DEFAULT 'simulated_chargeback',
    confirmed_at TIMESTAMPTZ NOT NULL,
    delay_hours DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_labels_confirmed_at ON labels (confirmed_at);
ALTER TABLE predictions ADD COLUMN IF NOT EXISTS label_confirmed_at TIMESTAMPTZ;
"""

# Transactions eligible for labeling: old enough, and not already labeled.
SELECT_UNLABELED_SQL = """
SELECT t.txn_id, t.user_id, t.amount, t.merchant_category, t.channel,
       t.ip_country, t.txn_timestamp
FROM transactions t
LEFT JOIN labels l ON l.txn_id = t.txn_id
WHERE l.txn_id IS NULL
  AND t.txn_timestamp <= (now() - (%(min_hours)s || ' hours')::interval)
ORDER BY t.txn_timestamp ASC
LIMIT %(batch_size)s;
"""

INSERT_LABEL_SQL = """
INSERT INTO labels (txn_id, is_fraud, label_source, confirmed_at, delay_hours)
VALUES (%(txn_id)s, %(is_fraud)s, %(label_source)s, %(confirmed_at)s, %(delay_hours)s)
ON CONFLICT (txn_id) DO NOTHING;
"""

UPDATE_PREDICTIONS_SQL = """
UPDATE predictions
SET label_confirmed_at = %(confirmed_at)s
WHERE txn_id = %(txn_id)s;
"""


from common.feature_columns import _home_country_for_user

def simulate_ground_truth(txn: dict, rng: random.Random) -> bool:
    """Synthetic ground-truth fraud label. NOT a real fraud model — this
    exists solely to give the pipeline a plausible, reproducible-ish label
    stream to train/monitor against in the absence of a real chargeback
    feed. Base rate ~BASE_FRAUD_RATE, boosted by a few naive risk signals.
    """
    score = BASE_FRAUD_RATE

    if txn["amount"] is not None and txn["amount"] > 1000:
        score += 0.15
    if txn["amount"] is not None and txn["amount"] > 5000:
        score += 0.25

    home_country = _home_country_for_user(txn["user_id"])
    if txn["ip_country"] and txn["ip_country"] != home_country:
        score += 0.20

    if txn["channel"] == "card_not_present":
        score += 0.03
    if txn["merchant_category"] in ("electronics", "gift_cards", "crypto"):
        score += 0.10

    score = min(score, 0.95)
    return rng.random() < score


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_LABELS)
    logger.info("labels table ensured")


def run_once(rng: random.Random) -> int:
    """One polling pass. Returns number of labels written."""
    written = 0
    with get_conn() as conn:
        with dict_cursor(conn) as cur:
            cur.execute(
                SELECT_UNLABELED_SQL,
                {"min_hours": LABEL_DELAY_MIN_HOURS, "batch_size": BATCH_SIZE},
            )
            rows = cur.fetchall()

        if not rows:
            logger.debug("no eligible unlabeled transactions this pass")
            return 0

        now = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            for row in rows:
                txn_age_hours = (now - row["txn_timestamp"]).total_seconds() / 3600.0
                # Only "confirm" a label once its randomly-assigned delay
                # has elapsed, so labels trickle in realistically rather
                # than all landing the instant the min-delay threshold passes.
                assigned_delay = rng.uniform(LABEL_DELAY_MIN_HOURS, LABEL_DELAY_MAX_HOURS)
                if txn_age_hours < assigned_delay:
                    continue

                is_fraud = simulate_ground_truth(row, rng)
                confirmed_at = now
                params = {
                    "txn_id": row["txn_id"],
                    "is_fraud": is_fraud,
                    "label_source": "simulated_chargeback",
                    "confirmed_at": confirmed_at,
                    "delay_hours": round(assigned_delay, 3),
                }
                cur.execute(INSERT_LABEL_SQL, params)
                cur.execute(
                    UPDATE_PREDICTIONS_SQL,
                    {"txn_id": row["txn_id"], "confirmed_at": confirmed_at},
                )
                written += 1

    if written:
        logger.info("wrote %d labels this pass", written)
    return written


def main() -> None:
    seed = os.environ.get("SIMULATOR_SEED")
    rng = random.Random(int(seed)) if seed is not None else random.Random()
    logger.info(
        "feedback.simulate_labels starting: delay=[%s,%s]h poll=%ss batch=%s base_rate=%s",
        LABEL_DELAY_MIN_HOURS,
        LABEL_DELAY_MAX_HOURS,
        POLL_INTERVAL_SECONDS,
        BATCH_SIZE,
        BASE_FRAUD_RATE,
    )
    # Ensure the labels table and predictions.label_confirmed_at column exist
    # once at startup, not on every polling cycle.
    with get_conn() as conn:
        ensure_schema(conn)
    while True:
        try:
            run_once(rng)
        except Exception:
            logger.exception("error during labeling pass, will retry next interval")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
