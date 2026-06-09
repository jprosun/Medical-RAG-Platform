FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    python3-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools wheel

# Install deps (from service folder)
COPY services/rag-orchestrator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY services/utils ./utils

# Copy service code
COPY services/rag-orchestrator/app ./app
COPY services/rag-orchestrator/guardrails ./guardrails

# Make sure Python can import "/app/utils" and model caches are writable by
# the non-root runtime user.
ENV PYTHONPATH=/app \
    HF_HOME=/home/appuser/.cache/huggingface \
    HF_HUB_CACHE=/home/appuser/.cache/huggingface/hub \
    SENTENCE_TRANSFORMERS_HOME=/home/appuser/.cache/sentence-transformers

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p "$HF_HOME" "$HF_HUB_CACHE" "$SENTENCE_TRANSFORMERS_HOME" \
    && chown -R appuser:appuser /app /home/appuser/.cache
USER 10001

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
