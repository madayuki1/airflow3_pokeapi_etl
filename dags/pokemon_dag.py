from datetime import datetime, timedelta
from airflow.sdk import dag, task, TaskGroup
from pathlib import Path
import requests
import yaml
from zoneinfo import ZoneInfo
from datetime import datetime
import os
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import logging
import glob

from include.extract import  ApiRequest
from include.app_utils import get_config
import include.transform
from include.airflow_gcs_loader import DataManager

current_path = Path(__file__).resolve()
root_path = current_path.parents[1]
data_path = root_path / 'data'
sql_path = current_path.parents[1] / 'include' / 'sql'

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ID = os.getenv('PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')
DATASET_NAME = os.getenv('DATASET_NAME')
AIRFLOW_PROJ_DIR = os.getenv ('AIRFLOW_PROJ_DIR')
GCS_CONN_ID = "GOOGLE_CLOUD_DEFAULT"

@dag(
    default_args=default_args,
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pokemon"],
    description="Pokemon ETL with airflow, dbt, and GCS",
)
def pokemon_etl():
    data_manager = DataManager(conn_id=GCS_CONN_ID)
    logger = logging.getLogger('airflow.task')

    @task(task_id="check_api_response")
    def check_api_response():
        url = 'https://pokeapi.co/api/v2'
        response = requests.Session().get(url)

        if response.status_code==200:
            print('API Reached')
        else:
            raise ValueError(f"API not reached, status code = {response.status_code}")

    def extract_and_load_raw_data(file_name, endpoint, transform_class, call_mode, batch_size):
        
        def download_file():
            data_manager.download_all(
                bucket_name=BUCKET_NAME,
                glob_pattern=f'{file_name}__*.json',
                local_file_name=data_path 
            )
        
        def single_call(endpoint, transform_class):
            now = datetime.now(ZoneInfo("Asia/Singapore"))
            glob_string = f'{file_name}__{now.strftime("%Y-%m-%d__%H-%M-%S")}.json'
            data = ApiRequest(endpoint).extractData()
            transformed_data = transform_class(data).raw()
            if transformed_data is None:
                return
            transformed_data.to_json(
                data_path / glob_string,
                orient='records',
                lines=True
            )
            data_manager.upload_file(
                bucket_name = BUCKET_NAME,
                cloud_file_name = glob_string,
                local_file_name = data_path / glob_string
            )
        
        def batch_call(endpoint, transform_class, batch_size):
            offset = 0

            print(f"Starting batch call for {endpoint}.")
            while True:
                now = datetime.now(ZoneInfo("Asia/Singapore"))
                glob_string = f'{file_name}__{now.strftime("%Y-%m-%d__%H-%M-%S")}.json'
                batch_data = ApiRequest(endpoint).extractData(offset=offset, limit=batch_size)

                if not batch_data:
                    print("No more data found. Ending batch.")
                    break

                transformed_batch = transform_class(batch_data).raw()
                if transformed_batch is None:
                    continue

                transformed_batch = transformed_batch.convert_dtypes()
                # logger.warning(f'trans batch: {transformed_batch['accuracy']}') 

                transformed_batch.to_json(
                    data_path / glob_string,
                    orient='records',
                    lines=True
                )
                data_manager.upload_file(
                    bucket_name = BUCKET_NAME,
                    cloud_file_name = glob_string,
                    local_file_name = data_path / glob_string
                )

                if len(batch_data) < batch_size:
                    print("Last batch received. Ending batch.")
                    break

                offset = offset + batch_size
            print(f"Finished batch call for {endpoint}.")

        def upload_schema(name):
            data_manager.upload_file(
                bucket_name=BUCKET_NAME,
                cloud_file_name=f'schema/{name}.json',
                local_file_name=data_path / f'schema/{name}.json'
            )

        @task(task_id=f'extract_{file_name}')
        def extract_data():
            download_file()
            if call_mode == 'single':   
                single_call(endpoint, transform_class)
            elif call_mode == 'batch':
                batch_call(endpoint, transform_class, batch_size)
            upload_schema(file_name)

        return extract_data()
        
    @task(task_id='get_gcs_file_list')
    def get_files():
        table_name = yaml.safe_load(
            get_config('etl_plan.yml').read_text()
        )['etl_plan']
        file_and_table_name = []
        for name, params in table_name.items():
            files = data_manager.get_list_files(
                        bucket_name=BUCKET_NAME,
                        glob_pattern=f'{name}__*'
                        )
            file_and_table_name.append(
                {
                    "source_objects": files,
                    "schema_object": f'schema/{name}.json',
                    "destination_project_dataset_table": f'raw.{name}'
                }
            )
        logger.warning(f'files: {file_and_table_name}')
        return file_and_table_name

    gcs_to_bq = GCSToBigQueryOperator.partial(
        task_id="gcs_to_bq_pokeapi",
        bucket=BUCKET_NAME,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        ignore_unknown_values=True,
        external_table=False,
        schema_fields=None,
        autodetect=False,
        # deferrable=True,
    ).expand_kwargs(
        get_files()
    )

    with TaskGroup("raw_layer") as raw_group:
        bronze_config = yaml.safe_load(
            get_config('etl_plan.yml').read_text()
        )['etl_plan']
        tasks = {}

        for name, params in bronze_config.items():
            tasks[name] = extract_and_load_raw_data(
                file_name = name,
                endpoint=params['endpoint'],
                transform_class=getattr(include.transform, params['transform_class']),
                call_mode=params['call_mode'],
                batch_size=params['batch_size']
            )

        for name, params in bronze_config.items():
            for dependencies in params.get('dependencies', []):
                if dependencies not in tasks:
                    continue
                tasks[dependencies] >> tasks[name]


    (
        check_api_response()
        >> raw_group
        >> gcs_to_bq
    )

pokemon_etl()