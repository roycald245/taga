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
    predicates_to_bdts_mapping: Dict[str, str]
    default_bdt: Optional[str]


class Model(BaseModel):
    tagging: Optional[Dict[str, List[BdtInstance]]] = {}
    anonymous_tagging: Optional[Dict[str, List[BdtInstance]]] = {}
    lambdas_props: Optional[Dict[str, Dict[str, Any]]] = {}
    conditions: Optional[Dict[str, ConditionalTagging]] = {}
