from typing import Union, Literal, List, Dict, Optional, Any

from pydantic import BaseModel

RAW = 'raw'
CANONIZED = 'canonized'
TRANSLITERATED = 'transliterated'
TRANSLATED = 'translated'
CONCATED = 'concated'
CONSTANT = 'constant'
COLUMN = 'column'
POINT = 'point'
START = 'start'
END = 'end'


class Reference(BaseModel):
    value: str
    type: Union[Literal[COLUMN], Literal[CONSTANT]]


class BdtInstance(BaseModel):
    form: Union[Literal[RAW], Literal[CANONIZED], Literal[TRANSLITERATED], Literal[TRANSLATED]] = RAW
    references: List[Reference]
    roles: Optional[List[str]] = []
    entity_id: Optional[str]
    date_type: Optional[Union[Literal[POINT], Literal[START], Literal[END]]]
    original_columns: Optional[List[str]] = []


class ConditionalTagging(BaseModel):
    condition_column: str
    effected_column: str
    bdt_to_predicates_mapping: Dict[str, List[str]]
    default_tagging: Optional[str]


class Tagging(Dict[str, List[BdtInstance]]):
    pass


class Model(BaseModel):
    tagging: Optional[Tagging]
    anonymous_tagging: Optional[Tagging]
    lambdas_props: Optional[Dict[str, Dict[str, Any]]] = {}
    conditions: Optional[List[ConditionalTagging]] = []
