import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.python_operator import PythonOperator

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
from callables import insert_to_redshift
from utils import (
    TABLE_EXISTS_SQL_PATH,
    CREATE_DB_SQL_PATH,
    FULL_SCHEMA,
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
    # upsert
    task_1 >> task_2
    task_2 >> task_3
