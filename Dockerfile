FROM apache/airflow:2.10.5-python3.11

# Install Java (JRE) and procps (for `ps`) as root, then clean up apt lists
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
