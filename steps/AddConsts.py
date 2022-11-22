from model import Model
from steps.IStep import IStep
import pyspark.sql.functions as F


class AddConsts(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df):
        for bdt_name, instances in self.model.tagging.items():
            for instance in instances:
                for ref in instance.references:
                    if ref.type == 'constant':
                        df = df.withColumn(bdt_name, F.lit(ref.value))
        return df, self.model
