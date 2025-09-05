from typing import List, Union
import pandas as pd
import inspect
import json
import glob
from pathlib import Path


from include.app_utils import (
    create_dataframe,
    add_timestamp,
    add_ingestion_date,
    validate_columns,
    get_short_effect_en,
    get_dict_key,
)

def convert_json_to_parquet(raw_path: str, file_name):
    files = glob.glob(str(f'{raw_path}/{file_name}__*'))
    df_list = []
    for file in files:
        df = pd.read_json(file)
        df_list.append(df)
    all_df = pd.concat(df_list)
    all_df.to_parquet(f'{raw_path}/parquet/{file_name}.parquet')

class BaseTransform:
    def __init__(self, data: Union[List[dict], pd.DataFrame]):
        self.data = data

    def raw(self):
        df = create_dataframe(self.data)
        if df is None:
            return None
        df = df.convert_dtypes()
        return df
    
    def bronze(self):
        return add_ingestion_date(self.data)

class TypeTransform(BaseTransform):
    def silver(self):
        df = add_timestamp(self.data)
        try:
            col_to_drop = ["generation", "game_indices", "names", "moves", "pokemon"]
            validate_columns(
                df, col_to_drop, context="drop", caller_frame=inspect.currentframe()
            )

            df = df.drop(columns=col_to_drop)
        except ValueError as e:
            print(e)
        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "type_id"}
        )
        return df


class MoveTransform(BaseTransform):

    def silver(self):
        df = add_timestamp(self.data)
        try:
            col_to_drop = ["generation"]
            validate_columns(
                df, col_to_drop, context="drop", caller_frame=inspect.currentframe()
            )
            df['damage_class'] = df['damage_class'].apply(lambda x: get_dict_key(x, 'name'))
            df['type'] = df['type'].apply(lambda x: get_dict_key(x, 'name'))

            df = df.drop(columns=col_to_drop)
        except ValueError as e:
            print(e)
        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "move_id"}
        )
        return df


class AbilityTransform(BaseTransform):

    def silver(self):
        df = add_timestamp(self.data)
        try:
            col_to_drop = ["generation", "names", "pokemon", "effect_changes"]
            validate_columns(
                df, col_to_drop, context="drop", caller_frame=inspect.currentframe()
            )

            df["short_effect_en"] = df["effect_entries"].apply(get_short_effect_en)
            df = df.drop(columns=col_to_drop)

        except ValueError as e:
            print(e)

        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "ability_id"}
        )
        return df

class PokemonTransform(BaseTransform):
    
    def silver(self):
        pokemon_df = add_timestamp(self.data)
        try:
            col_to_drop = ["cries", "game_indices"]
            validate_columns(
                pokemon_df,
                col_to_drop,
                context="drop",
                caller_frame=inspect.currentframe(),
            )

            pokemon_df = pokemon_df.drop(columns=col_to_drop)
            pokemon_df['species'] = pokemon_df['species'].apply(lambda x: get_dict_key(x, 'url'))
        except ValueError as e:
            print(e)
        pokemon_moves = []
        pokemon_abilities = []
        pokemon_types = []
        pokemon_stats = []

        for _, row in pokemon_df.iterrows():
            pokemon_id = row["id"]
            types = (
                json.loads(row["types"])
                if isinstance(row["types"], str)
                else row["types"]
            )
            moves = (
                json.loads(row["moves"])
                if isinstance(row["moves"], str)
                else row["moves"]
            )
            abilities = json.loads(
                row["abilities"]
                if isinstance(row["abilities"], str)
                else row["abilities"]
            )
            stats = json.loads(
                row["stats"] if isinstance(row["stats"], str) else row["stats"]
            )

            for type in types:
                type_url = type["type"]["url"]
                type_id = type_url.split("/")[-2]
                slot = type["slot"]
                pokemon_types.append(
                    {"pokemon_id": pokemon_id, "type_id": type_id, "slot": slot}
                )

            for move in moves:
                move_url = move["move"]["url"]
                move_id = move_url.split("/")[-2]
                pokemon_moves.append({"pokemon_id": pokemon_id, "move_id": move_id})

            for ability in abilities:
                ability_url = ability["ability"]["url"]
                ability_id = ability_url.split("/")[-2]
                pokemon_abilities.append(
                    {"pokemon_id": pokemon_id, "ability_id": ability_id}
                )

            for stat in stats:
                stat_name = stat["stat"]["name"].replace("-", "_")
                base_stat = stat["base_stat"]
                effort = stat["effort"]
                pokemon_stats.append(
                    {
                        "pokemon_id": pokemon_id,
                        "stat_name": stat_name,
                        "base_stat": base_stat,
                        "effort": effort,
                    }
                )

        pokemon_df = pokemon_df.drop(columns=["types", "moves", "abilities"])

        pokemon_types_df = pd.DataFrame(pokemon_types)
        pokemon_moves_df = pd.DataFrame(pokemon_moves)
        pokemon_abilities_df = pd.DataFrame(pokemon_abilities)
        pokemon_stats_df = pd.DataFrame(pokemon_stats)

        yield "pokemon", pokemon_df
        yield "pokemon_types", pokemon_types_df
        yield "pokemon_moves", pokemon_moves_df
        yield "pokemon_abilities", pokemon_abilities_df
        yield "pokemon_stats", pokemon_stats_df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "pokemon_id"}
        )
        return df


class PokemonSpeciesTransform(BaseTransform):

    def silver(self):
        df = add_timestamp(self.data)
        try:
            col_to_drop = [
                "color",
                "egg_groups",
                "evolution_chain",
                "evolves_from_species",
                "forms_switchable",
                "growth_rate",
                "habitat",
                "shape",
                "varieties",
            ]
            validate_columns(
                df, col_to_drop, context="drop", caller_frame=inspect.currentframe()
            )

            # df["short_effect_en"] = df["effect_entries"].apply(get_short_effect_en)
            df['generation'] = df['generation'].apply(lambda x: get_dict_key(x, 'url'))
            df = df.drop(columns=col_to_drop)

        except ValueError as e:
            print(e)

        return df

    def gold(self):
        df = self.data.drop(columns=["created_at", "updated_at"]).rename(
            columns={"id": "ability_id"}
        )
        return df
