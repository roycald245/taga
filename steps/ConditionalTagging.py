from model import Model
from steps.IStep import IStep
import pyspark.sql.functions as f
from pyspark.sql import DataFrame


class ConditionalTagging(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame) -> (DataFrame, Model):
        for condition in self.model.conditions:
            for bdt, values in condition.bdt_to_values.items():
                df = df.withColumn(bdt, f.when(f.col(condition.condition_column).isin(values),
                                               f.col(condition.column_to_tag.value)).otherwise(
                    condition.default_tagging))
        return df, self.model
