from model import Model
from steps.IStep import IStep
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


class ConditionalTagging(IStep):

    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame):
        for condition in self.model.conditions:
            for bdt, values in condition.bdt_to_values.items():
                df = df.withColumn(bdt, F.when(F.col(condition.condition_column).isin(values),
                                               F.col(condition.column_to_tag.value)).otherwise(
                    condition.default_tagging))
        return df, self.model
