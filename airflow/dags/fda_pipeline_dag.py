from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ak',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

PROJECT_DIR = '/opt/airflow/project'
DBT_DIR = f'{PROJECT_DIR}/FDA_Pipeline'

with DAG(
    'fda_pipeline_dag',
    default_args=default_args,
    description='Daily FDA drug shortage pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    ingest_shortages  = BashOperator(
        task_id='ingest_shortages',
        bash_command=f'python {PROJECT_DIR}/ingestion/fda_shortages.py',
    )

    ingest_recalls = BashOperator(
        task_id='ingest_recalls',
        bash_command=f'python {PROJECT_DIR}/ingestion/fda_recalls.py',
    )
    ingest_approvals = BashOperator(
        task_id='ingest_approvals',
        bash_command=f'python {PROJECT_DIR}/ingestion/fda_approvals.py',
    )
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && /home/airflow/.local/bin/dbt run --profiles-dir {DBT_DIR}',
        env={
            'DB_HOST': 'host.docker.internal',
            'DB_PORT': '5432',
            'DB_USER': 'postgres',
            'DB_PASSWORD': '123',
            'DB_NAME': 'pharma_db',
        }
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && /home/airflow/.local/bin/dbt test --profiles-dir {DBT_DIR}',
        env={
            'DB_HOST': 'host.docker.internal',
            'DB_PORT': '5432',
            'DB_USER': 'postgres',
            'DB_PASSWORD': '123',
            'DB_NAME': 'pharma_db',
        }
    )

    [ingest_shortages, ingest_recalls, ingest_approvals] >> dbt_run >> dbt_test
