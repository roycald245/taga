from collections import defaultdict
from copy import deepcopy
from typing import Tuple, Dict, List

from pyspark.sql import DataFrame
import pyspark.sql.functions as f

from model import Model, COLUMN
from steps.IStep import IStep


def _map_columns_to_bdt_instances(model: Model) -> Dict[str, List[str]]:
    columns_to_bdts = defaultdict(list)
    for bdt_name, bdt_instances in model.tagging.items():
        for instance_i, instance in enumerate(bdt_instances):
            if len(instance.references) == 1:
                if instance.references[0].type == COLUMN:
                    columns_to_bdts[instance.references[0].value].append(f'{bdt_name}~{instance_i}')
    return columns_to_bdts


class DuplicateRepeatedColumnsStep(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame) -> (DataFrame, Model):
        model_copy = self.model.copy(deep=True)
        columns_to_bdt_instances = _map_columns_to_bdt_instances(model_copy)

        for column, bdt_instances in columns_to_bdt_instances.items():
            if len(bdt_instances) > 1:
                for bdt_instance in bdt_instances:
                    duplicated_column_name = f'{column}~{bdt_instance}'
                    df = df.withColumn(duplicated_column_name, f.col(column))
                    bdt_name, instance_i = bdt_instance.split('~')
                    model_copy.tagging[bdt_name][int(instance_i)].references[0].value = duplicated_column_name

        return df, model_copy
