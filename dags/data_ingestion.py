import os
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from include.task import get_scf_data
from dotenv import load_dotenv


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
    max_pages = int(os.getenv('MAX_PAGES', 20))

    _EL_scf_data = PythonOperator(
        task_id="get_SCF_Issues",
        python_callable=get_scf_data,
        op_args=[base_url, max_pages]  
    )

    transform_issues = DockerOperator(
        task_id ='transform_issues',
        image='airflow/issues--transformation',
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



    _EL_scf_data >> transform_issues

get_SCF()
