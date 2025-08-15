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

# ---- Installing BigQuery connectors în pyspark/jars ----
USER root
ARG BQ_VER=0.36.3
ARG BQ_JAR="spark-bigquery-with-dependencies_2.12-${BQ_VER}.jar"
RUN py_site=$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))") \
 && mkdir -p "${py_site}/jars" \
 && curl -fSL "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${BQ_VER}/${BQ_JAR}" \
      -o "/tmp/${BQ_JAR}" \
 && mv "/tmp/${BQ_JAR}" "${py_site}/jars/${BQ_JAR}" \
 && chown -R airflow:root "${py_site}/jars"
USER airflow
