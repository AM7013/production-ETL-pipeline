FROM python:3.13-slim AS base

RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y openjdk-21-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

WORKDIR /opt/prefect

COPY requirements.txt .
RUN pip install --upgrade setuptools wheel
RUN pip install --no-cache-dir \
    "prefect>=2.19.0" \
    "pandas>=2.0.0" \
    "pandas-gbq>=0.20.0" \
    "pyspark>=4.1.3" \
    psycopg2-binary \
    python-dotenv \
    prefect-shell


COPY orchestration/ ./orchestration/
COPY dbt/ ./dbt/

ENV PYTHONPATH=/opt/prefect/orchestration/Prefect

WORKDIR /opt/prefect/orchestration/Prefect
