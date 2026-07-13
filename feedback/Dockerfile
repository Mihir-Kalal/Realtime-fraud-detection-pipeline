FROM python:3.11-slim

WORKDIR /app

COPY feedback/requirements.txt feedback/requirements.txt
RUN pip install --no-cache-dir -r feedback/requirements.txt

COPY common/ common/
COPY feedback/ feedback/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "feedback/simulate_labels.py"]
