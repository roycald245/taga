import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from model import Model
from steps.IStep import IStep


class ConditionalTagging(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame) -> (DataFrame, Model):
        for condition in self.model.conditions:
            for bdt, predicates in condition.bdt_to_predicates_mapping.items():
                cases = f.when(f.col(condition.condition_column).isin(predicates), f.col(condition.effected_column)) \
                    .otherwise(condition.default_tagging)
                df = df.withColumn(bdt, cases)
                return df, self.model
