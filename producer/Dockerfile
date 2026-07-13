FROM python:3.11-slim

WORKDIR /app

# Bring in the shared common/ package so `from common.schemas import ...`
# and `from common.stream_reader import ...` resolve inside the container.
COPY common/ /app/common/
COPY producer/requirements.txt /app/producer/requirements.txt
RUN pip install --no-cache-dir -r /app/producer/requirements.txt

COPY producer/ /app/producer/

ENV PYTHONPATH=/app

CMD ["python", "producer/main.py"]
