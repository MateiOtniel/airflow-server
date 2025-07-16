# 1) Bază oficială Airflow + Python 3.11
FROM apache/airflow:2.10.5-python3.11

USER root

# 2) Instalează Java & build-tools necesare pentru PySpark
RUN apt-get update \
  && apt-get install -y default-jdk gcc g++ \
  && rm -rf /var/lib/apt/lists/*

# 3) Definește AIRFLOW_HOME și structura de directoare
ENV AIRFLOW_HOME=/opt/airflow
RUN mkdir -p ${AIRFLOW_HOME}/{dags,logs,plugins,scripts,data}

# 4) Copiază fișierul cu dependențe Python
COPY requirements.txt /tmp/requirements.txt

# 5) Instalează pachetele Python ca utilizatorul airflow
#    (Pip ca root nu e permis de imaginea oficială)
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER airflow

RUN pip install --upgrade pip \
 && pip install --no-cache-dir \
      --constraint "${CONSTRAINT_URL}" \
      -r /tmp/requirements.txt

# 6) Revino la root pentru a copia codul și seta permisiuni
USER root

# 7) Copiază-ți codul în directorul definit
COPY dags/    ${AIRFLOW_HOME}/dags/
COPY scripts/ ${AIRFLOW_HOME}/scripts/
COPY data/    ${AIRFLOW_HOME}/data/
COPY plugins/ ${AIRFLOW_HOME}/plugins/

# 8) Asigură permisiunile corecte pentru utilizatorul airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

# 9) Rulează containerul ca airflow
USER airflow
EXPOSE 8080
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]
