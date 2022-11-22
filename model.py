from typing import Union, Literal, List, Dict, Optional, Any

from pydantic import BaseModel


class Reference(BaseModel):
    value: str
    type: Union[Literal['column'], Literal['constant']]


class BdtInstance(BaseModel):
    form: Union[Literal['raw'], Literal['canonized'], Literal['transliterated'], Literal['translated']] = 'raw'
    references: List[Reference]
    roles: List[str] = []
    entity_id: Optional[str]
    date_type: Optional[Union[Literal['point'], Literal['start'], Literal['end']]]


class ConditionalTagging(BaseModel):
    condition_column: str
    columns_to_tag: List[Reference]
    values_mapping: Dict[str, str]
    default_tagging: Optional[str]


class Model(BaseModel):
    tagging: Dict[str, List[BdtInstance]]
    lambdas_props: Dict[str, Dict[str, Any]] = {}
    conditions: List[ConditionalTagging] = []
