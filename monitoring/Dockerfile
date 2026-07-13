FROM python:3.11-slim

WORKDIR /app

# monitoring/drift.py shells out to `python -m training.train` on drift
# breach, reusing Phase 3's training code exactly rather than duplicating
# it — so this image needs training/'s dependencies installed too.
COPY monitoring/requirements.txt monitoring/requirements.txt
COPY training/requirements.txt training/requirements.txt
RUN pip install --no-cache-dir -r monitoring/requirements.txt \
    && pip install --no-cache-dir -r training/requirements.txt

COPY common/ common/
COPY monitoring/ monitoring/
COPY training/ training/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV REPO_ROOT=/app

CMD ["python", "monitoring/drift.py"]
