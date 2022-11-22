from pyspark.sql import SparkSession

from model import Model
from test_model import TEST_MODEL
from workflow import execute_workflow

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('taga').getOrCreate()

    df = spark.read.csv('./test_table.csv', header=True)
    df, model = execute_workflow(df, Model(**TEST_MODEL))
    df.show()
