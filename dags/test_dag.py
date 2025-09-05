from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task, TaskGroup
from pathlib import Path
import requests
import yaml
from zoneinfo import ZoneInfo
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import pandas as pd
import pandas_gbq
from google.cloud import storage
import os
import logging
from docker.types import Mount

logger = logging.getLogger('airflow.task')

# from include.extract import  ApiRequest
from include.app_utils import get_config, get_class, get_schema_bigquery, get_ids
# from include.gcs_manager import GcsManager
from include.data_classes import PokemonSpecies
from include.transform import MoveTransform
from include.airflow_gcs_loader import DataManager 

current_path = Path(__file__).resolve()
root_path = current_path.parents[1]
data_path = root_path / 'data'
sql_path = current_path.parents[1] / 'include' / 'sql'

AIRFLOW_PROJ_DIR = os.getenv('AIRFLOW_PROJ_DIR')
PROJECT_ID = os.getenv('PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')
DATASET_NAME = os.getenv('DATASET_NAME')
GCS_CONN_ID = "GOOGLE_CLOUD_DEFAULT"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test"],
    description="Testing functionality",
)
def test_dag():
    # gcs_manager = GcsManager(project_id=PROJECT_ID, bucket_name=BUCKET_NAME)
    data_loader = DataManager(
        conn_id='GOOGLE_CLOUD_DEFAULT'
    )
    @task(task_id="Check_API_Response")
    def check_api_response():
        url = 'https://pokeapi.co/api/v2'
        response = requests.Session().get(url)

        if response.status_code==200:
            print('API Reached')
        else:
            raise ValueError(f"API not reached, status code = {response.status_code}")

    @task(task_id='get_files')
    def get_files():
        table_name = yaml.safe_load(
            get_config('bronze_etl_plan.yml').read_text()
        )['bronze_etl_plan']
        file_and_table_name = []
        for name, params in table_name.items():
            files = data_loader.get_list_files(
                        bucket_name=BUCKET_NAME,
                        glob=f'raw/{name}__*'
                    )
            file_and_table_name.append(
                {
                    "source_objects": files,
                    "destination_project_dataset_table": f'raw.{name}'
                }
            )
        logger.warning
        return file_and_table_name


    gcs_to_bq = GCSToBigQueryOperator.partial(
        task_id="gcs_to_bq_pokeapi",
        bucket=BUCKET_NAME,
        ignore_unknown_values=True,
        allow_jagged_rows=True,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        deferrable=True,
    ).expand_kwargs(
        get_files()
    )
    pd.DataFrame.to_gbq 
    @task(task_id='redownload_file')
    def download_file():
        data_loader.download_all(
            bucket_name=BUCKET_NAME,
            glob="raw/*.json",
            local_file_name=data_path / 'raw'
        )


    (
        check_api_response()
        # >> get_files()
        >> download_file()
        >> gcs_to_bq
        # >> run_dbt
        # >> bronze_layer()
    )

test_dag()