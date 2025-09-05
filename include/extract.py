import requests
from typing import List
import json
import yaml
import logging
from pathlib import Path
from dataclasses import dataclass, fields
from include.app_utils import get_ids

project_root = Path(__file__).resolve().parent.parent  # go 2 levels up
schema_path = project_root / "config" / "raw_schema.yml"

logger = logging.getLogger('airflow.task')

class ApiRequest:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.name = endpoint.split('/')[-1].replace('-', '_')

        with open(schema_path, "r") as file:
            raw_schema = yaml.safe_load(file)

        self.schema = raw_schema[self.name]


    # def filter_api_data(self, api_data, allowed_fields):
    #     return {key: value for key, value in api_data.items() if key in allowed_fields}

    def filter_api_data(self, api_data, allowed_fields):
        data = {}
        for key, data_type in allowed_fields.items():
            value = api_data[key]
            # logger.warning(f'key: {key}, value: {data_type}')
            if not value:
                data[key] = None
                continue
            if data_type == 'int':
                value = int(value)
                # logger.warning(f'value: {value}, went here')
            data[key] = value
        return data
        

    def extractData(self, offset: int = 0, limit: int = 20) -> List[dict]:
        data = []
        url = f"{self.endpoint}?offset={offset}&limit={limit}"
        ids = get_ids(
            match_glob=project_root / 'data' / f'{self.name}__*'
        )

        response = requests.Session().get(url).json()
        results = response.get("results")
        if not results:
            return None
        
        url = response.get("next")
        for item in results:
            id = int(item["url"].split("/")[-2])
            if id in ids:
                continue

            detail_response = requests.Session().get(f"{self.endpoint}/{id}").json()
            filtered_data = self.filter_api_data(detail_response, self.schema)
            data.append(filtered_data)
        if not data:
            return None

        return data