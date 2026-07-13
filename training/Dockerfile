FROM python:3.11-slim

WORKDIR /app

# System deps needed by psycopg2-binary's runtime + xgboost's libgomp requirement
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY training/requirements.txt /app/training/requirements.txt
RUN pip install --no-cache-dir -r /app/training/requirements.txt

# common/ is baked in per the same convention used by producer/feature_engine
COPY common/ /app/common/
COPY training/ /app/training/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# On-demand job, not a long-running service -- matches Phase 0's
# `docker compose run --rm training python training/train.py` convention.
CMD ["python", "training/train.py"]
