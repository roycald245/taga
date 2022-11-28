import uuid

from pyspark.sql import DataFrame

from model import Model, Reference, COLUMN, CONSTANT
from steps.IStep import IStep
import pyspark.sql.functions as F


class AddConsts(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame):
        for bdt_name, instances in self.model.tagging.items():
            for instance in instances:
                new_refs = instance.references
                for ref in instance.references:
                    if ref.type == CONSTANT:
                        column_name = str(uuid.uuid4())
                        df = df.withColumn(column_name, F.lit(ref.value))
                        new_refs.append(Reference(type=COLUMN, value=column_name))
                instance.references = new_refs

        return df, self.model
