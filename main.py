from pyspark.sql import SparkSession

from Model import Model
from workflow import execute_workflow

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('taga').getOrCreate()

    df = spark.read.csv('./test_table.csv', header=True)
    df, model = execute_workflow(df, Model({
        'fullest_name': {
            'references': [
                {
                    'type': 'column',
                    'value': 'fullest_name1'
                },
                {
                    'type': 'column',
                    'value': 'fullest_name2'
                }
            ]
        },
        'imei': {
            'references': [
                {'type': 'column', 'value': 'imei'}
            ]
        },
        'id': {
            'references': [
                {'type': 'column', 'value': 'id'}
            ]
        },
        'pstn': {
            'references': [
                {'type': 'column', 'value': 'pstn'}
            ]
        }
    }))
    df.show()
