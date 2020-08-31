from __future__ import annotations

import json
from abc import ABCMeta, abstractmethod
from io import BytesIO
from typing import Type


class Serializable(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
    def from_bytes(cls, data: bytes) -> Serializable:
        pass

    @abstractmethod
    def to_bytes(self):
        pass


class SchematicModel(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def json_schema(cls) -> dict:
        """ Returns json schema of the model """

    @abstractmethod
    def to_dict(self) -> dict:
        """ Returns self dictionary """


class SchematicRecord:
    def __init__(self, datatype: Type[SchematicModel]):
        self.datatype = datatype
        self.schema = datatype.json_schema()
        self.registered_schemas = {}

    @abstractmethod
    def read(self, in_stream: BytesIO, schema: dict):
        pass

    @abstractmethod
    def write(self, out_stream: BytesIO, object_dict: dict):
        pass

    @abstractmethod
    def parse(self, avro_schema: dict):
        pass

    @property
    @abstractmethod
    def schema(self):
        pass

    @schema.setter
    @abstractmethod
    def schema(self, datatype_schema: dict):
        pass

    def from_bytes(
        self, in_stream: BytesIO, schema_id: int, schema_str: str
    ) -> SchematicModel:
        schema = self.registered_schemas.get(schema_id, None)
        if schema is None:
            schema = self.parse(json.loads(schema_str))
            self.registered_schemas[schema_id] = schema
        return self.datatype(**self.read(in_stream, schema))

    def to_bytes(self, out_stream: BytesIO, data_model: SchematicModel) -> bytes:
        self.write(out_stream, data_model.to_dict())
