from collections import defaultdict
from copy import deepcopy
from typing import Tuple, Dict, List

from pyspark.sql import DataFrame

from model import Model


def duplicate_repeated_columns(model: Model, df: DataFrame) -> Tuple[Model, DataFrame]:
    df_copy = df
    model_copy = deepcopy(model)
    columns_to_bdt_refs = _map_columns_to_bdt_refs(model_copy)
    for column, bdt_refs in columns_to_bdt_refs.items():
        for bdt_ref in bdt_refs:
            bdt_name, instance_i, reference_i = bdt_ref.split('~')
            duplicated_column_name = f'{column}~{bdt_name}'
            model_copy.tagging[bdt_name][instance_i].references[reference_i].value = duplicated_column_name
            df_copy = df_copy.withColumn(duplicated_column_name, df_copy[column])
    return model_copy, df_copy


def _map_columns_to_bdt_refs(model: Model) -> Dict[str, List[str]]:
    columns_to_bdts = defaultdict(list)
    for bdt_name, bdt_instances in model.tagging.items():
        for instance_i, instance in enumerate(bdt_instances):
            for reference_i, reference in enumerate(instance.references):
                if reference.type == 'column':  # TOOD: refactor to Enum
                    columns_to_bdts[reference.value].append(f'{bdt_name}~{instance_i}~{reference_i}')
    return columns_to_bdts
