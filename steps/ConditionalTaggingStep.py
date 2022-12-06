import itertools
import uuid
from collections import defaultdict
from typing import List, Dict

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from model import Model, ConditionalTagging, BdtInstance, Reference, COLUMN, TagOption
from steps.IStep import IStep


def _handle_regular_options(df: DataFrame,
                            conditional_tagging: ConditionalTagging,
                            generated_tagging=None) -> (DataFrame, Dict[str, List[BdtInstance]]):
    if generated_tagging is None:
        generated_tagging = defaultdict(list)

    for option in conditional_tagging.options:
        generated_column_name = f'{option.__str__()}~{str(uuid.uuid4())}'
        df = df.withColumn(generated_column_name,
                           f.when(f.col(conditional_tagging.condition_column).isin(option.predicates),
                                  f.col(conditional_tagging.effected_column))
                           .otherwise(None))
        generated_tagging[option.bdt_name].append(BdtInstance(
            references=[Reference(value=generated_column_name, type=COLUMN)],
            roles=option.roles,
            entity_id=option.entity_id,
            date_type=option.date_type
        ))

    return df, generated_tagging


def summarize_all_predicates(conditional_tagging: ConditionalTagging) -> List[str]:
    flatmapped_predicates = itertools.chain(*[option.predicates for option in conditional_tagging.options])
    return list(set(flatmapped_predicates))


def _handle_default_option(df: DataFrame,
                           conditional_tagging: ConditionalTagging,
                           generated_tagging=None) -> (DataFrame, Dict[str, List[BdtInstance]]):
    if generated_tagging is None:
        generated_tagging = defaultdict(list)

    if conditional_tagging.default_option:
        all_the_predicates = summarize_all_predicates(conditional_tagging)
        option = conditional_tagging.default_option
        generated_column_name = f'{option.__str__()}~{str(uuid.uuid4())}'
        df = df.withColumn(generated_column_name,
                           f.when(~f.col(conditional_tagging.condition_column).isin(all_the_predicates),
                                  f.col(conditional_tagging.effected_column))
                           .otherwise(None))
        generated_tagging[option.bdt_name].append(BdtInstance(
            references=[Reference(value=generated_column_name, type=COLUMN)],
            roles=option.roles,
            entity_id=option.entity_id,
            date_type=option.date_type
        ))

    return df, generated_tagging


def _handle_conditional_tagging(
        df: DataFrame,
        conditional_tagging: ConditionalTagging,
        generated_tagging=None) -> (DataFrame, Dict[str, List[BdtInstance]]):
    if generated_tagging is None:
        generated_tagging = defaultdict(list)
    df, generated_tagging = _handle_regular_options(df, conditional_tagging, generated_tagging)
    df, generated_tagging = _handle_default_option(df, conditional_tagging, generated_tagging)
    return df, generated_tagging


class ConditionalTaggingStep(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame) -> (DataFrame, Model):
        model = self.model.copy(deep=True)
        generated_tagging = defaultdict(list)

        for conditional_tagging in self.model.conditions:
            df, generated_tagging = _handle_conditional_tagging(df, conditional_tagging, generated_tagging)
        for bdt_name, instances in model.tagging.items():
            generated_tagging[bdt_name].extend(instances)

        model.tagging = generated_tagging
        model.conditions = None
        return df, model
