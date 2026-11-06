FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

# JAVA_HOME résolu au build : le chemin diffère entre amd64 et arm64
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3

COPY requirements-spark.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

WORKDIR /app
