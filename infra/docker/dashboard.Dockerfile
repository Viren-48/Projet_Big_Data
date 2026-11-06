FROM python:3.11-slim-bookworm

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

COPY requirements-dashboard.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-dashboard.txt

WORKDIR /app
