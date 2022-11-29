import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from model import Model
from steps.IStep import IStep


class ConditionalTagging(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame) -> (DataFrame, Model):
        for condition in self.model.conditions:
            for bdt, predicates in condition.predicates_to_bdts_mapping.items():
                df = df.withColumn(bdt,
                                   f.when(f.col(condition.condition_column).isin(predicates),
                                          f.col(condition.effected_column)) \
                                   .otherwise(condition.default_bdt)) # Should be none
                return df, self.model
