from pyspark.sql import DataFrame

from model import Model


class IStep:
    def process(self, df: DataFrame) -> (DataFrame, Model):
        pass
