import uuid
from collections import defaultdict

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from model import Model, COLUMN, Reference, Tagging, CONSTANT, BdtInstance, CONCATED
from steps.IStep import IStep


class ConcatBdt(IStep):
    def __init__(self, model: Model, concat_delimiter=' '):
        self.model = model
        self.concat_delimiter = concat_delimiter

    def process(self, df: DataFrame) -> (DataFrame, Model):
        model = self.model
        df, model.tagging = self._process_tagging(df, model.tagging)
        df, model.anonymous_tagging = self._process_tagging(df, model.anonymous_tagging)
        return model, df

    def _process_tagging(self, df: DataFrame, input_tagging: Tagging) -> (DataFrame, Tagging):
        output_tagging = defaultdict(list)
        for bdt_name, bdt_instances in input_tagging.items():
            for instance in bdt_instances:
                if len(instance.references) > 1:
                    df, concated_bdt_instance = self._apply_concat_on_instance(df, instance)
                    output_tagging[bdt_name].append(concated_bdt_instance)
                else:
                    output_tagging[bdt_name].append(instance)
        return df, Tagging(output_tagging)

    def _apply_concat_on_instance(self, df: DataFrame, instance: BdtInstance) -> (DataFrame, BdtInstance):
        original_columns = []
        concat_refs = []

        for index, ref in enumerate(instance):
            if ref.type == COLUMN:
                original_columns.append(ref.value)
                concat_refs.append(f.col(ref.value))
            elif ref.type == CONSTANT:
                concat_refs.append(f.lit(ref.value))
            if index != len(instance.references) - 1:
                concat_refs.append(f.lit(self.concat_delimiter))

        column_name = str(uuid.uuid4())
        df = df.withColumn(column_name, f.concat(*concat_refs))

        output_instance = instance.copy(deep=True)
        output_instance.form = CONCATED
        output_instance.references = [Reference(type=COLUMN, value=column_name)]
        output_instance.original_columns = original_columns
        return df, output_instance
