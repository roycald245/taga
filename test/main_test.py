import json
import unittest

from pyspark.sql import SparkSession

from model import Model
from workflow import execute_workflow


class MainTest(unittest.TestCase):
    def test_end_to_end(self):
        spark = SparkSession.builder.master('local').appName('taga').getOrCreate()

        with open(r'test_model.json', mode='r', encoding='utf-8') as f:
            test_model = json.load(f)

        df = spark.read.csv('./test_table.csv', header=True)
        df, model = execute_workflow(df, Model(**test_model))
        df.show()
        print(model.dict())
