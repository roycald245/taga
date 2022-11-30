import uuid
from collections import defaultdict

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from model import Model
from steps.IStep import IStep


class ConditionalTagging(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame) -> (DataFrame, Model):
        for conditional_tagging in self.model.conditions.values():
            bdts_to_predicates = defaultdict(list)
            for predicate, bdt in conditional_tagging.predicates_to_bdts_mapping:
                bdts_to_predicates[bdt].append(predicate)
            for bdt, predicates in bdts_to_predicates.items():
                generated_column_name =
        for condition_column, condition_tagging in self.model.conditions.items():
            for predicate, bdt in condition_tagging.predicates_to_bdts_mapping.items():
                uuid.uuid4()
                df = df.withColumn(bdt,
                                   f.when(f.col(condition_column) == f.lit(predicate),
                                          f.col(condition.effected_column)) \
                        None)  # Should be none
                return df, self.model
