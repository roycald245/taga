from functools import reduce

from model import Model
from steps.IStep import IStep
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def generate_condition_from_values(values, condition_column):
    case = ""
    for i, value in enumerate(values):
        case += f"{condition_column} == {value}"
        if i < len(values) - 1:
            case += " OR "
    return F.expr(case)


class ConditionalTagging(IStep):

    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame):
        for condition in self.model.conditions:
            for bdt, values in condition.values_mapping.items():
                df = df.withColumn(bdt, F.when(generate_condition_from_values(values, condition.condition_column),
                                               F.col(condition.column_to_tag.value)).otherwise(
                    condition.default_tagging))
        return df
