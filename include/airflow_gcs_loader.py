from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import BytesIO
import glob
import pandas as pd
import logging

from include.app_utils import get_pandas_function

logger = logging.getLogger('airflow.task')

class DataManager():
    def __init__(self, conn_id):
        self.hook = GCSHook(
            gcp_conn_id=conn_id
        ) 
    
    def download_file(self, bucket_name, cloud_file_name, local_file_name=None):
        kwargs = {
            "bucket_name": bucket_name,
            "object_name": cloud_file_name
        }
        if local_file_name:
            kwargs["filename"] = local_file_name
        try:
            return self.hook.download(**kwargs)
        except Exception as e:
            logger.warning(f'Download file fail: {e}')
            return None

    def download_all(self, bucket_name, glob_pattern,  local_file_name=None):
        all_data_in_bucket = self.get_list_files(
            bucket_name=bucket_name,
            glob_pattern=glob_pattern
        )
        local_file = glob.glob(str(local_file_name))
        try:
            for file in all_data_in_bucket:
                if file in local_file:
                    continue
                self.download_file(
                    bucket_name=bucket_name,
                    cloud_file_name=file,
                    local_file_name=f'{ local_file_name}/{file.split('/')[-1]}'
                )
        except Exception as e:
            logger.warning(f'Fail Downloading all: {e}')

    def upload_file(self, bucket_name, cloud_file_name, local_file_name):
        try:
            self.hook.upload(
                bucket_name=bucket_name,
                object_name=cloud_file_name,
                filename=local_file_name
            )
        except Exception as e:
            logger.warning(f'Uploading file failed: {e}')

    def upload_folder(self, bucket_name, local_folder_name):
        local_files = glob.glob(str(local_folder_name))
        cloud_files = self.get_list_files(bucket_name, '*.json')
        try:
            for file in local_files:
                if file in cloud_files:
                    continue
                self.upload_file(
                    bucket_name=bucket_name,
                    cloud_file_name=file,
                    local_file_name=file
                )
        except Exception as e:
            logger.warning(f'Fail upload folder: {e}')

    
    def get_list_files(self, bucket_name, glob_pattern):
        return self.hook.list(
            bucket_name=bucket_name,
            match_glob=glob_pattern
        )