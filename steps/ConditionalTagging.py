import uuid
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from model import Model, ConditionalTagging, BdtInstance, Reference, COLUMN, TagOption
from steps.IStep import IStep


def _map_tag_options_by_names(tag_options: List[TagOption]) -> Dict[str, TagOption]:
    return {option.__str__(): option for option in tag_options}


def _group_predicates_by_tag_options(predicates_to_bdts_options: Dict[str, TagOption]) -> Dict[str, List[str]]:
    tags_to_predicates = defaultdict(list)
    for predicate, tag_option in predicates_to_bdts_options.items():
        tag_name = tag_option.__str__()
        tags_to_predicates[tag_name].append(predicate)
    return tags_to_predicates


def _handle_regular_options(df: DataFrame,
                            conditional_tagging: ConditionalTagging,
                            generated_tagging=None) -> (DataFrame, Dict[str, List[BdtInstance]]):
    if generated_tagging is None:
        generated_tagging = defaultdict(list)

    tags_to_predicates = _group_predicates_by_tag_options(conditional_tagging.predicates_to_tag_options)
    mapped_tag_options = _map_tag_options_by_names(list(conditional_tagging.predicates_to_tag_options.values()))

    for tag, predicates in tags_to_predicates.items():
        option = mapped_tag_options[tag]
        generated_column_name = f'{tag}~{str(uuid.uuid4())}'
        df = df.withColumn(generated_column_name,
                           f.when(f.col(conditional_tagging.condition_column).isin(predicates),
                                  f.col(conditional_tagging.effected_column))
                           .otherwise(None))
        generated_tagging[option.bdt_name].append(BdtInstance(
            references=[Reference(value=generated_column_name, type=COLUMN)],
            roles=option.roles,
            entity_id=option.entity_id,
            date_type=option.date_type
        ))

    return df, generated_tagging


def _handle_default_option(df: DataFrame,
                           conditional_tagging: ConditionalTagging,
                           generated_tagging=None) -> (DataFrame, Dict[str, List[BdtInstance]]):
    if generated_tagging is None:
        generated_tagging = defaultdict(list)

    if conditional_tagging.default_tag_option:
        all_the_predicates = list(conditional_tagging.predicates_to_tag_options.keys())
        option = conditional_tagging.default_tag_option
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


class ConditionalTagging(IStep):
    def __init__(self, model: Model):
        self.model = model

    def process(self, df: DataFrame) -> (DataFrame, Model):
        generated_tagging = defaultdict(list)
        model = deepcopy(self.model)
        for conditional_tagging in self.model.conditions.values():
            df, generated_tagging = _handle_conditional_tagging(df, conditional_tagging, generated_tagging)
        for bdt_name, instances in generated_tagging.items():
            model.tagging[bdt_name].extend(instances)
        model.conditions = None
        return df, model
