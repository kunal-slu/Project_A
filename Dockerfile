# Dockerfile for PySpark ETL Jobs
# Supports running Spark jobs in containerized environments (ECS, Kubernetes, etc.)

FROM amazoncorretto:17-alpine AS base

# Install system dependencies
RUN apk add --no-cache \
    python3 \
    python3-dev \
    py3-pip \
    curl \
    bash \
    && ln -sf python3 /usr/bin/python

WORKDIR /app

# Copy requirements
COPY requirements.txt requirements-dev.txt ./

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt && \
    pip3 install --no-cache-dir -r requirements-dev.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/
COPY aws/ ./aws/

# Set Python path
ENV PYTHONPATH=/app/src:$PYTHONPATH
ENV SPARK_HOME=/opt/spark

# Install Spark (lightweight version for container)
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3

RUN curl -sSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | \
    tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Add Spark to PATH
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:$PATH

# Default command
CMD ["python3", "-m", "project_a.pipeline.run_pipeline"]
