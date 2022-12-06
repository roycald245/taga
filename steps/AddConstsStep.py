import uuid
from collections import defaultdict
from typing import List, Dict

from pyspark.sql import DataFrame

from model import Model, Reference, COLUMN, CONSTANT, BdtInstance
from steps.IStep import IStep
import pyspark.sql.functions as f


def _apply_add_consts_on_tagging(df: DataFrame, tagging: Dict[str, List[BdtInstance]]) -> (
        DataFrame, Dict[str, List[BdtInstance]]):
    output_tagging = defaultdict(list)
    for bdt_name, instances in tagging.items():
        for instance in instances:
            output_instance = instance.copy(deep=True)
            if len(instance.references) == 1 and instance.references[0].type == CONSTANT:
                generated_column_name = f'{bdt_name}~{str(uuid.uuid4())}'
                df = df.withColumn(generated_column_name, f.lit(instance.references[0].value))
                output_instance.references = [Reference(type=COLUMN, value=generated_column_name)]
                output_tagging[bdt_name].append(output_instance)
            else:
                output_tagging[bdt_name].append(output_instance)
    return df, output_tagging


class AddConstsStep(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame) -> (DataFrame, Model):
        output_model = self.model.copy(deep=True)
        df, output_model.tagging = _apply_add_consts_on_tagging(df, self.model.tagging)
        df, output_model.anonymous_tagging = _apply_add_consts_on_tagging(df, self.model.anonymous_tagging)
        return df, output_model
