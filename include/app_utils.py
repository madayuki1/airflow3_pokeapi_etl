from typing import List, Union
from pandas import DataFrame
from datetime import datetime
import pandas as pd
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Type
import importlib
import json
import glob
import dataclasses
import pyarrow as pa
from google.cloud import bigquery
import logging
from io import BytesIO
import os

logger = logging.getLogger('airflow.task')

def test_connectivity():
    logger.warning('guess we are in bois')

def create_dataframe(data: Union[List[dict], DataFrame]) -> DataFrame:
    df = DataFrame(data)    
    if df.empty:
        return None
    return df

def to_datetime(date_string: str) -> str:
    date_format = [
        "%Y-%m-%d %H:%M:%S",  # Example: 2023-01-10 12:30:45
        "%Y-%m-%d"            # Example: 2023-01-10"
    ]

    for format in date_format:
        try:
            parsed_date = datetime.strptime(date_string, format)
            return parsed_date.isoformat()
        except ValueError:
            continue

def save_list_to_csv(data: list, filename="output.csv"):
    """
    Saves a list of JSON objects (dicts) to a CSV file.

    Parameters:
    - data (list): A list of dictionaries containing JSON data.
    - filename (str): The name of the CSV file to save.

    Example:
        response = [{"id": 1, "name": "Bulbasaur"}, {"id": 2, "name": "Charmander"}]
        save_list_to_csv(response, "pokemon.csv")
    """
    if not isinstance(data, list):
        raise ValueError("Input must be a list of dictionaries")

    try:
        df = pd.DataFrame(data)  # Convert list to DataFrame
        df.to_csv(filename, index=False)  # Save as CSV
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving list to CSV: {e}")

def add_timestamp(data: pd.DataFrame)-> pd.DataFrame:
    current_time = datetime.now(ZoneInfo("Asia/Singapore"))
    df = (
        data.assign(created_at=current_time)
        .assign(updated_at=current_time)
    )
    return df    

def add_ingestion_date(data: pd.DataFrame)-> pd.DataFrame:
    current_time = datetime.now(ZoneInfo("Asia/Singapore"))
    df = (
        data.assign(ingestion_date=current_time)
    )
    return df  

def get_short_effect_en(effect_entries_string:str):
    try:
        entries = json.loads(effect_entries_string)
        for entry in entries:
            if entry.get('language', {}).get('name') == 'en':
                return entry.get('short_effect')
    except (json.JSONDecodeError, TypeError):
        return None
    
def get_dict_key(json_string:str, key_name:str):
    try:
        data_dict = json.loads(json_string)
        dict_key = data_dict.get(key_name)
        if not dict_key:
            return
        if key_name == 'url':
            return dict_key.split('/')[-2]
        else:
            return dict_key
    except (json.JSONDecodeError, TypeError):
        return None
    
def get_generation(generation_string:str):
    try:
        generation = json.loads(generation_string)
        url_string = generation.get('url')
        if not url_string:
            return
        id = generation.get('url').split('/')[-2]
        return id
    
    except (json.JSONDecodeError, TypeError):
        return None
    
def get_type(type_string:str):
    try:
        type = json.loads(type_string)
        name_string = type.get('name')
        if not name_string:
            return
        name = type.get('name')
        return name
    
    except (json.JSONDecodeError, TypeError):
        return None
    
def get_species(species_string:str):
    try:
        species = json.loads(species_string)
        url_string = species.get('url')
        if not url_string:
            return
        id = species.get('url').split('/')[-2]
        return id
    
    except (json.JSONDecodeError, TypeError):
        return None

def validate_columns(
    data: DataFrame, 
    column_to_check: List[str], 
    context: str = "use",
    caller_frame=None)-> None:
    
    missing = [col for col in column_to_check if col not in data.columns]
    if not missing:
        return

    if caller_frame:
        class_name = caller_frame.f_locals.get('self').__class__.__name__
        method_name = caller_frame.f_code.co_name
        error_prefix = f"[{class_name}.{method_name}]"
    else:
        error_prefix = "[Validation]"
    
    action = 'Required' if context == 'use' else 'Droppable'
    raise ValueError(f'{error_prefix}; {action} columns missing: {missing}')

def get_config(config_name: str) -> Path:
    return  Path(__file__).parent.parent / "config" / f"{config_name}"

def get_class(classname: str, module: str) -> Type:
    module_name = importlib.import_module(module)
    try:
        return getattr(module_name, classname)
    except AttributeError:
        raise ValueError(f'Class {classname} not found in module {module_name}')

def get_schema_pyarrow(schema: dict):
    column_types = []

    for name, data_type in schema.items():
        if data_type == "int":
            column_types.append((name, pa.int64()))
            # data_type = pa.int64()
        elif data_type == "float":
            column_types.append((name, pa.float64()))
            # data_type = pa.float64()
        elif data_type == "bool":
            column_types.append((name, pa.string()))
            # data_type = pa.string()
        else:
            column_types.append((name, pa.string()))
            # data_type = pa.string()
        # column_types.append((name, field.type))


    return pa.schema(column_types)

def get_schema_bigquery(schema: dict):
    column_types = []

    for field in schema:
        if field['data_type'] == 'RECORD':
            nested_fields = get_schema_bigquery(field['fields']) if 'fields' in field else []
            column_types.append(
                bigquery.SchemaField(
                    field['name'],
                    field['data_type'],
                    mode=field['mode'],
                    fields=nested_fields
                )
            )
        else:
            column_types.append(
                bigquery.SchemaField(
                    field['name'],
                    field['data_type'],
                    mode=field['mode']
                )
            )

    return column_types

def get_pandas_function(mode:str, extension:str):
    read_functions = {
            'json': lambda path: pd.read_json(path, orient='records', lines=True),
            'parquet': lambda path: pd.read_parquet(path)
        }
    write_functions = {
        }
    
    if mode=="read":
        if extension in read_functions:
            return read_functions[extension]
    elif mode=='write':
        if extension in write_functions:
            return write_functions[extension]

def get_ids(match_glob, id_column='id'):
    files = glob.glob(str(match_glob))
    all_ids = set()
    read_function = get_pandas_function(
        mode='read',
        extension='json'
    )
    for file in files:
        df = read_function(
            path = file
        )
        if df.empty:
            return all_ids

        if id_column in df.columns:
            all_ids.update(df[id_column].dropna().unique())
    return all_ids