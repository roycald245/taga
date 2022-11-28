from typing import Union, Literal, List, Dict, Optional, Any

from pydantic import BaseModel

RAW = 'raw'
CANONIZED = 'canonized'
TRANSLITERATED = 'transliterated'
TRANSLATED = 'translated'
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


class ConditionalTagging(BaseModel):
    condition_column: str
    column_to_tag: Reference
    bdt_to_values: Dict[str, List[str]]
    default_tagging: Optional[str]


class Model(BaseModel):
    tagging: Dict[str, List[BdtInstance]]
    lambdas_props: Optional[Dict[str, Dict[str, Any]]] = {}
    conditions: Optional[List[ConditionalTagging]] = []
