from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from io import BytesIO

import fastavro
from avro_schema.convertor import JsonSchema
from pydantic import BaseModel
from pydantic.main import ModelMetaclass


class Serializable:
    @staticmethod
    def from_bytes(cls, data: bytes) -> Serializable:
        raise NotImplementedError()

    def to_bytes(self):
        raise NotImplementedError()


class SchematicSerializable:
    @classmethod
    def get_model_schema(cls) -> str:
        raise NotImplementedError()

    @classmethod
    def from_bytes(
        cls, in_stream: BytesIO, schema_id: int, schema: str
    ) -> SchematicSerializable:
        raise NotImplementedError()

    def to_bytes(self) -> bytes:
        raise NotImplementedError()


class AvroSchemaMetaClass(ModelMetaclass):
    def __new__(cls, name, bases, dct):
        new_class = super().__new__(cls, name, bases, dct)
        generated_avro = JsonSchema(new_class.schema()).to_avro()
        new_class._avro_schema = fastavro.parse_schema(generated_avro)
        return new_class


class AvroSerializable(SchematicSerializable, BaseModel, metaclass=AvroSchemaMetaClass):
    _encoders = {}

    @classmethod
    def get_model_schema(cls) -> str:
        return json.dumps(JsonSchema(cls.schema()).to_avro())

    @classmethod
    def from_bytes(
        cls, in_stream: BytesIO, schema_id: int, schema: str
    ) -> AvroSerializable:
        if schema_id not in AvroSerializable._encoders:
            schema_json = json.loads(schema)
            avro_schema = fastavro.parse_schema(schema_json)
            AvroSerializable._encoders[schema_id] = avro_schema
        deser_data = fastavro.schemaless_reader(
            in_stream, AvroSerializable._encoders[schema_id]
        )
        return cls(**deser_data)

    def to_bytes(self) -> bytes:
        with BytesIO() as out_stream:
            fastavro.schemaless_writer(out_stream, self._avro_schema, self.dict())
            return out_stream.getvalue()
