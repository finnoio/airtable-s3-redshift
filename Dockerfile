FROM apache/airflow:2.3.0
USER airflow
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

