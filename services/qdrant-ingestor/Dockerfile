FROM python:3.11-slim

WORKDIR /app
COPY services/qdrant-ingestor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/qdrant-ingestor/app ./app
COPY services/qdrant-ingestor/ingest_kaggle_precomputed.py ./services/qdrant-ingestor/ingest_kaggle_precomputed.py
COPY services/utils ./services/utils
ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "app.ingest"]
