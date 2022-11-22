from Model import Model
from steps.IStep import IStep
import pyspark.sql.functions as F


class ConcatBdt(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df):
        for bdt_name, bdt_body in self.model.items():
            if len(bdt_body.get('references')) > 1:
                concat_columns = []
                for ref in bdt_body.get('references'):
                    if ref.get('type') == 'column':
                        concat_columns.append(F.col(ref.get('value')))
                    else:
                        concat_columns.append(F.lit(ref.get('value')))
                    concat_columns.append(F.lit(' '))

                df = df.withColumn(bdt_name, F.concat(*concat_columns))

        return df, self.model


