FROM apache/airflow:latest

USER root

# 1. Install Java 17 and procps
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Set Java Home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 3. FIX THE PATH: Ensure the airflow binary is found first
# This prevents the "/entrypoint: line 20: airflow: command not found" error
ENV PATH="/home/airflow/.local/bin:${PATH}"

USER airflow

# 4. Install PySpark and providers
RUN pip install --no-cache-dir \
    pyspark==4.0.0 \
    apache-airflow-providers-apache-spark \
    snowflake-connector-python