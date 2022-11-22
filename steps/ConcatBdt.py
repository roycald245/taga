from copy import deepcopy
from model import Model
from steps.IStep import IStep
import pyspark.sql.functions as F


class ConcatBdt(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df):
        for bdt_name, bdt_instances in self.model.tagging.items():
            bdt_instances_new = deepcopy(bdt_instances)
            for instance in bdt_instances_new:
                if len(instance.references) > 1:
                    concat_columns = []
                    for index, ref in enumerate(instance.references):
                        if ref.type == 'column':
                            concat_columns.append(F.col(ref.value))
                        else:
                            concat_columns.append(F.lit(ref.value))
                        if index != len(instance.references)-1:
                            concat_columns.append(F.lit(' '))

                    df = df.withColumn(bdt_name, F.concat(*concat_columns))

        return df, self.model
