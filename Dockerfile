FROM apache/airflow:2.6.0
COPY ./dag_callables /opt/airflow/dag_callables
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt