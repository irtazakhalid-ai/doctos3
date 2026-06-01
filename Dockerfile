FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard.py .
COPY kaggle_to_s3.py .

EXPOSE 8080

# Use gunicorn in production.
# --workers 1        : single process so only one background upload thread starts
# --threads 4        : handle concurrent HTTP requests within that process
# --timeout 300      : requests (e.g. /files/skipped with 197k entries) may be slow
# --bind uses $PORT  : Railway injects PORT at runtime
CMD ["sh", "-c", "gunicorn --workers 1 --threads 4 --timeout 300 --bind 0.0.0.0:${PORT:-8080} dashboard:app"]
