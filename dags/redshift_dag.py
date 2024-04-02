import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

# Add the the dag_callables package to the sys.path.
# I'm also normalizing the path to avoid getting import errors.
sys.path.insert(
    0,
    os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),  # ./dags
            os.pardir,                  # ./
            "dag_callables",            # ./dag_callables
        )
    ),
)
from callables import insert_to_redshift, get_average_bitcoin_price_category
from utils import (
    TABLE_EXISTS_SQL_PATH,
    CREATE_DB_SQL_PATH,
    FULL_SCHEMA,
    EMAIL_TO,
)


DEFAULT_ARGS = {
    'owner': 'NorberMV',
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
        default_args=DEFAULT_ARGS,
        dag_id="DAG_with_Redshift_Connection",
        description="First Redshift Connection DAG.",
        start_date=datetime(2021, 1, 1),
        schedule_interval="0 3 * * *",
        catchup=False,
        tags=['Redshift_DAG']
) as dag:
    # Adding this query in a dedicated task as the
    # Redshift operator seems to have issues with multi
    # statement sql queries described here:
    # https://github.com/aws/amazon-redshift-python-driver/issues/21#issue-821042437
    task_1 = RedshiftSQLOperator(
        task_id='setup__table_exists',
        redshift_conn_id="redshift_coder",
        sql=TABLE_EXISTS_SQL_PATH.read_text(),
    )
    task_2 = RedshiftSQLOperator(
        task_id='setup__create_table',
        redshift_conn_id="redshift_coder",
        sql=CREATE_DB_SQL_PATH.read_text(),
    )
    task_3 = PythonOperator(
        task_id='insert_data_to_redshift',
        python_callable=insert_to_redshift,
    )
    task_4 = PythonOperator(
        task_id='retrieve_btc_price',
        python_callable=get_average_bitcoin_price_category,
    )
    task_5 = EmailOperator(
        task_id='send_BTC_mail_alert',
        to=EMAIL_TO,
        subject='Airflow Bitcoin Price Alert!',
        html_content="{{ task_instance.xcom_pull(task_ids='retrieve_btc_price') }}",
    )
    # upsert
    task_1 >> task_2
    task_2 >> task_3
    task_3 >> task_4
    task_4 >> task_5
