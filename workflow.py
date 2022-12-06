from pyspark.sql import DataFrame

from model import Model
from steps import ConcatBdtsStep, AddConstsStep, ConditionalTaggingStep, DuplicateRepeatedColumnsStep


def execute_workflow(df: DataFrame, model: Model) -> (DataFrame, Model):
    df, model = ConcatBdtsStep(model).process(df)
    df, model = AddConstsStep(model).process(df)
    df, model = ConditionalTaggingStep(model).process(df)
    df, model = DuplicateRepeatedColumnsStep(model).process(df)
    return df, model
