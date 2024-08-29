import os
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.task import get_scf_data
from dotenv import load_dotenv


dotenv_path = '/workspaces/SeeClickFix_Pipeline/.env'
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

    _EL_scf_data

get_SCF()
