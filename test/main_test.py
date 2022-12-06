import json
import unittest

from pyspark.sql import SparkSession

from model import Model
from workflow import execute_workflow

TEST_MODEL = {
    "tagging": {
        "fullest_name": [
            {
                "form": "raw",
                "references": [
                    {
                        "type": "column",
                        "value": "fullest_name1"
                    },
                    {
                        "type": "constant",
                        "value": "WHAT"
                    },
                    {
                        "type": "column",
                        "value": "fullest_name2"
                    }
                ]
            }
        ],
        "arena": [
            {
                "form": "raw",
                "references": [
                    {
                        "type": "constant",
                        "value": "gugu"
                    }
                ]
            }
        ]
    },
    "conditions": [
        {
            "condition_column": "phone_2_type",
            "effected_column": "phone2",
            "options": [
                {
                    "predicates": [
                        "kav", "land"
                    ],
                    "bdt_name": "landline_phone_number",
                    "roles": [
                        "residency"
                    ]
                },
                {
                    "predicates": [
                        "kav_2"
                    ],
                    "bdt_name": "landline_phone_number",
                    "roles": [
                        "employment"
                    ]
                },
                {
                    "predicates": [
                        "_m", "mob"
                    ],
                    "bdt_name": "mobile_phone_number"
                }
            ],
            "default_option": {
                "bdt_name": "generic_phone_number"
            }
        },
        {
            "condition_column": "emp_time_type",
            "effected_column": "emp_time",
            "options": [
                {
                    "predicates": [
                        "s"
                    ],
                    "bdt_name": "employment_date",
                    "date_type": "start"
                },
                {
                    "predicates": [
                        "e"
                    ],
                    "bdt_name": "employment_date",
                    "date_type": "end"
                }
            ]
        }
    ]
}


class MainTest(unittest.TestCase):
    def test_end_to_end(self):
        spark = SparkSession.builder.master('local').appName('taga').getOrCreate()
        df = spark.read.csv('../test_table.csv', header=True)
        df, model = execute_workflow(df, Model(**TEST_MODEL))
        df.show()
        print(model.dict())
