import uuid

import pyspark.sql.functions as F

from model import Model, COLUMN, Reference
from steps.IStep import IStep


class ConcatBdt(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df):
        for bdt_name, bdt_instances in self.model.tagging.items():
            for instance in bdt_instances:
                if len(instance.references) > 1:
                    concat_columns = []
                    for index, ref in enumerate(instance.references):
                        if ref.type == COLUMN:
                            concat_columns.append(F.col(ref.value))
                        else:
                            concat_columns.append(F.lit(ref.value))
                        if index != len(instance.references) - 1:
                            concat_columns.append(F.lit(' '))
                    column_name = str(uuid.uuid4())
                    df = df.withColumn(column_name, F.concat(*concat_columns))
                    instance.references = [*instance.references, Reference(type=COLUMN, value=column_name)]

        return df, self.model
