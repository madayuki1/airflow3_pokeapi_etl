FROM apache/airflow:3.0.2

USER root


USER airflow

COPY requirements.txt /
RUN pip install "apache-airflow==3.0.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.12.txt"
RUN pip install --no-cache-dir -r /requirements.txt