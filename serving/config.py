"""
serving/config.py — reconstructed per Phase 4's documented contract (real
file wasn't visible in this chat; reconcile with your actual copy if it
differs). FEATURE_COLUMNS is copied verbatim from Phase 3's handoff and
MUST stay byte-for-byte identical to training/train.py and
monitoring/feature_columns.py.
"""
import os

from common.feature_columns import FEATURE_COLUMNS

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
REDIS_MAX_CONNECTIONS = int(os.environ.get("REDIS_MAX_CONNECTIONS", "50"))
FLAG_THRESHOLD = float(os.environ.get("FLAG_THRESHOLD", "0.5"))
AB_SPLIT_PERCENTAGE = int(os.environ.get("AB_SPLIT_PERCENTAGE", "50"))
SCORING_MODE = os.environ.get("SCORING_MODE", "ab_test").lower()
ASYNC_DB_WRITE = os.environ.get("ASYNC_DB_WRITE", "true").lower() == "true"

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://frauduser:fraudpass@localhost:5432/frauddb")
POSTGRES_POOL_MIN_SIZE = int(os.environ.get("POSTGRES_POOL_MIN_SIZE", "2"))
POSTGRES_POOL_MAX_SIZE = int(os.environ.get("POSTGRES_POOL_MAX_SIZE", "10"))
