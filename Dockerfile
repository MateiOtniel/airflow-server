FROM apache/airflow:2.10.5-python3.11

USER root

# Setează AIRFLOW_HOME la folderul implicit din imagine
ENV AIRFLOW_HOME=/opt/airflow

# Asigură structura de directoare
RUN mkdir -p ${AIRFLOW_HOME}/dags \
             ${AIRFLOW_HOME}/logs \
             ${AIRFLOW_HOME}/plugins \
             ${AIRFLOW_HOME}/scripts \
             ${AIRFLOW_HOME}/data

# Instalează provider-ii și dependențele necesare
RUN pip install --no-cache-dir \
      apache-airflow-providers-apache-spark \
      pyspark>=3.1.3 \
      google-cloud-storage>=2.0.0 \
      pyarrow>=4.0.0 \
      psycopg2-binary>=2.9.0 \
      pandas>=1.0.5

# Copiază-ți codul în directorul definit
COPY dags/     ${AIRFLOW_HOME}/dags/
COPY plugins/  ${AIRFLOW_HOME}/plugins/   # dacă ai plugin-uri
COPY scripts/  ${AIRFLOW_HOME}/scripts/
COPY data/     ${AIRFLOW_HOME}/data/

# Dă permisiuni corecte
RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow

EXPOSE 8080
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]
