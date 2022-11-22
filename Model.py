from typing import Dict, List, Union, Literal

from pydantic import BaseModel


class Reference(BaseModel):
    value: str
    type: Union[Literal['column'], Literal['constant']]


class BDTBody(BaseModel):
    references: List[Reference]


class Model(Dict[str, BDTBody]):
    pass
