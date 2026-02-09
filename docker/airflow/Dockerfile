FROM apache/airflow:2.8.4-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    pyarrow==17.0.0 \
    pandas==2.1.4 \
    boto3==1.34.162 \
    botocore==1.34.162 \
    dbt-core>=1.6,<2.0 \
    dbt-spark>=1.6,<2.0
