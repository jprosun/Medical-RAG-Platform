FROM python:3.11-slim

# Prevent Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# ---- system deps (minimal but sufficient) ----
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ---- Python deps ----
COPY services/streamlit-ui/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ---- Copy app ----
COPY services/streamlit-ui/app.py ./app.py
COPY services/utils ./utils

# ---- Streamlit runtime config ----
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
ENV STREAMLIT_SERVER_ENABLE_CORS=false
ENV STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false

# ---- Prepare runtime dirs for non-root ----
RUN useradd -u 10001 -m appuser \
 && mkdir -p /home/appuser/.streamlit /tmp \
 && chown -R appuser:appuser /home/appuser /tmp /app

USER 10001

EXPOSE 8501

# ---- Explicit run command (important) ----
CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0"]
