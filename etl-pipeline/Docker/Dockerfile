# Base image
FROM python:3.10-slim AS builder


LABEL maintainer="A.M <vexteleta@gmail.com>"
LABEL version="1.0"
LABEL description="Custom Airflow image for ETL pipeline with quality gating and Spark support"
LABEL org.opencontainers.image.source="https://github.com/AM7013/production-ETL-pipeline"
LABEL org.opencontainers.image.licenses="MIT"

RUN useradd -m -u 1000 appuser 
USER appuser
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Final image
FROM python:3.10-slim 

WORKDIR /app

RUN useradd -m -u 1000 appuser 
COPY --from=builder /home/appuser/.local /home/appuser/.local
COPY etl_pipeline.py .
COPY cleaned_data_v1.csv .


RUN chown -R appuser:appuser /app

# Set the PATH environment variable to include the local bin directory for the appuser
ENV PATH=/home/appuser/.local/bin:$PATH

USER appuser


CMD ["python", "etl_pipeline.py"]
