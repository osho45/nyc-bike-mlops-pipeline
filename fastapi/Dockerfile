FROM python:3.11-slim
WORKDIR /app

# Ensure pip is up to date before installing dependencies
RUN pip install --no-cache-dir --upgrade pip

# REMOVED: duckdb, polars
# ADDED: psycopg2-binary for enterprise-grade transactional engine mapping
RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.32.1 pandas==2.2.3 pyarrow==19.0.1 psycopg2-binary==2.9.10 duckdb==1.1.3

COPY fastapi/main.py /app/main.py

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
