from pyspark.sql import DataFrame

from model import Model
from steps import ConcatBdt, AddConsts, ConditionalTagging


def execute_workflow(df: DataFrame, model: Model) -> (DataFrame, Model):
    df, model = ConcatBdt(model).process(df)
    df, model = AddConsts(model).process(df)
    df, model = ConditionalTagging(model).process(df)
    return df, model
