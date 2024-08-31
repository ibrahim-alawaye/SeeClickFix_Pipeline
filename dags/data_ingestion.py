import os
from airflow.decorators import dag
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from include.task import get_scf_data
from dotenv import load_dotenv
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from datetime import datetime
import requests
import sqlalchemy

from include.task import _get_transformed_data
dotenv_path = '/usr/local/airflow/.env'
load_dotenv(dotenv_path)

@dag(
    start_date=datetime(2024, 5, 2),
    schedule_interval='@daily',
    catchup=False,
    tags=['SCF_Data_EL']
)
def get_SCF():

    base_url = os.getenv('BASE_URL')
    max_pages = int(os.getenv('MAX_PAGES', 5))

    _EL_scf_data = PythonOperator(
        task_id="get_SCF_Issues",
        python_callable=get_scf_data,
        op_args=[base_url, max_pages]  
    )

    transform_issues = DockerOperator(
        task_id ='transform_issues',
        image='airflow/issues---transformation',
        container_name='transform--issues',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="_EL_scf_data") }}'
        }
    )

    get_transformed_data = PythonOperator(
        task_id='get_transformed_data',
        python_callable=_get_transformed_data,
        op_kwargs={
            'path': '{{ task_instance.xcom_pull(task_ids="store_raw_data") }}'
        }

    )

    load_data_postgres = aql.load_file(
        task_id='load_dw',
        input_file=File(path='{{ task_instance.xcom_pull(task_ids="get_transformed_data") }}',conn_id='minio'),
        output_table=Table(
            name='scf_issues',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            ),
            columns = [
                sqlalchemy.Column('id', sqlalchemy.BigInteger, primary_key=True),
                sqlalchemy.Column('reporter_name', sqlalchemy.String),
                sqlalchemy.Column('created_at', sqlalchemy.DateTime),
                sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
                sqlalchemy.Column('issue_status', sqlalchemy.String),
                sqlalchemy.Column('issue_summary', sqlalchemy.Text),
                sqlalchemy.Column('latitude', sqlalchemy.Float),
                sqlalchemy.Column('longitude', sqlalchemy.Float),
                sqlalchemy.Column('issue_address', sqlalchemy.String),
                sqlalchemy.Column('issue_url', sqlalchemy.String),
                sqlalchemy.Column('reporter_role', sqlalchemy.String),
                sqlalchemy.Column('organization', sqlalchemy.String),
                sqlalchemy.Column('title', sqlalchemy.String),
                sqlalchemy.Column('is_private', sqlalchemy.Boolean),
                sqlalchemy.Column('rating', sqlalchemy.Float),
                sqlalchemy.Column('Date', sqlalchemy.Date),
                ]
        )
    )






    _EL_scf_data >> transform_issues >> get_transformed_data >> load_data_postgres

get_SCF()
