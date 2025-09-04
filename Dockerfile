FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      default-jre-headless \
      procps \
 && rm -rf /var/lib/apt/lists/*

# Point JAVA_HOME at the distro’s default JRE and add java to PATH
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Explicitly set AIRFLOW_HOME
ENV AIRFLOW_HOME=/opt/airflow

# Switch back to airflow user for installing Python deps
USER airflow

# Copy and install Python requirements
COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install --no-cache-dir --upgrade pip setuptools \
 && pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
      -r ${AIRFLOW_HOME}/requirements.txt \
 && rm ${AIRFLOW_HOME}/requirements.txt


# ---- BigQuery + GCS (shaded) jars pentru Spark 3.5.x / Scala 2.12 ----
USER root
ARG SPARK_BQ_VER=0.42.4
ARG GCS_VER=hadoop3-2.2.11
RUN set -eux; \
    py_site=$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))"); \
    jars_dir="${py_site}/jars"; \
    mkdir -p "${jars_dir}"; \
    curl -fSL "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${SPARK_BQ_VER}/spark-bigquery-with-dependencies_2.12-${SPARK_BQ_VER}.jar" \
      -o "${jars_dir}/spark-bigquery-with-dependencies_2.12-${SPARK_BQ_VER}.jar"; \
    curl -fSL "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/${GCS_VER}/gcs-connector-${GCS_VER}-shaded.jar" \
      -o "${jars_dir}/gcs-connector-${GCS_VER}-shaded.jar"; \
    chown -R airflow:root "${jars_dir}"
USER airflow
