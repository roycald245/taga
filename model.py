from typing import Union, Literal, List, Dict, Optional, Any, Dict

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


class TagOption(BaseModel):
    bdt_name: str
    roles: Optional[List[str]] = []
    entity_id: Optional[str]
    date_type: Optional[Union[Literal[POINT], Literal[START], Literal[END]]]

    def __str__(self) -> str:
        tag = self.bdt_name
        if self.date_type:
            tag = f'{tag}~{self.date_type}'
        if self.roles:
            tag = f"{tag}~{'-'.join(self.roles)}"
        if self.entity_id:
            tag = f'{tag}~{self.entity_id}'
        return tag


class ConditionalTagging(BaseModel):
    condition_column: str
    effected_column: str
    predicates_to_tag_options: Dict[str, TagOption]
    default_tag_option: Optional[TagOption]


class Model(BaseModel):
    tagging: Optional[Dict[str, List[BdtInstance]]] = {}
    anonymous_tagging: Optional[Dict[str, List[BdtInstance]]] = {}
    lambdas_props: Optional[Dict[str, Dict[str, Any]]] = {}
    conditions: Optional[List[ConditionalTagging]]
