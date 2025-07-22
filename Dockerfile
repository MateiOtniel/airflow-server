FROM apache/airflow:2.10.5-python3.11

# Explicitly set AIRFLOW_HOME so the warning goes away
ENV AIRFLOW_HOME=/opt/airflow

# Switch to the airflow user (base image already created this user)
USER airflow

# Copy only requirements and install them as the airflow user
COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt

RUN pip install --no-cache-dir --upgrade pip setuptools \
 && pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
      -r ${AIRFLOW_HOME}/requirements.txt \
 && rm ${AIRFLOW_HOME}/requirements.txt
