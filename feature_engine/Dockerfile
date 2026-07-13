FROM python:3.11-slim

WORKDIR /app

COPY common/ ./common/
COPY feature_engine/requirements.txt ./feature_engine/requirements.txt
RUN pip install --no-cache-dir -r feature_engine/requirements.txt

COPY feature_engine/ ./feature_engine/
COPY graph_features/ ./graph_features/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "feature_engine/main.py"]
