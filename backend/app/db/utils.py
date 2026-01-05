import inspect
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, Union


class SDKModel(BaseModel):
    id: Optional[Union[str, int]] = Field(None, description="MongoDB document ID")
    # v2 config: ignore extra fields, allow name-based population
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    def __getattribute__(self, item: str):
        try:
            return super().__getattribute__(item)
        except AttributeError:
            # Look up declared fields (no code‐gen needed!)
            fields = super().__getattribute__("model_fields")
            if item in fields:
                field_info = fields[item]
                ann = field_info.annotation

                # Nested sub‐model? construct an empty one
                if inspect.isclass(ann) and issubclass(ann, BaseModel):
                    inst = ann.model_construct(_fields_set=set())
                    object.__setattr__(self, item, inst)
                    return inst

                # Primitive or anything else: just return None
                return None

            # truly unknown attribute
            raise

    @property
    def _id(self) -> Optional[Union[str, int]]:
        return self.id

    @_id.setter
    def _id(self, value: Optional[Union[str, int]]):
        self.id = value
