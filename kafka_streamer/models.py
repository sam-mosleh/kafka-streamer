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
    def to_bytes(self):
        raise NotImplementedError()

    @staticmethod
    def from_bytes(cls, data: bytes) -> Serializable:
        raise NotImplementedError()


class RawBytes(Serializable):
    def __init__(self, data: bytes):
        self.data = data

    def to_bytes(self) -> bytes:
        return self.data

    @classmethod
    def from_bytes(cls, data: bytes) -> RawBytes:
        return RawBytes(data)


class SchematicSerializable:
    _schema = None
    _encoders = {}

    def to_bytes(self, schema_id) -> bytes:
        raise NotImplementedError()

    @classmethod
    def from_bytes(cls, data: bytes) -> SchematicSerializable:
        pass


class AvroSchemaMetaClass(ModelMetaclass):
    def __new__(cls, name, bases, dct):
        new_class = super().__new__(cls, name, bases, dct)
        generated_avro = JsonSchema(new_class.schema()).to_avro()
        new_class._schema = json.dumps(generated_avro)
        new_class._avro_schema = fastavro.parse_schema(generated_avro)
        return new_class


class AvroSerializable(SchematicSerializable,
                       BaseModel,
                       metaclass=AvroSchemaMetaClass):
    def to_bytes(self, out_stream: BytesIO) -> bytes:
        fastavro.schemaless_writer(out_stream, self._avro_schema, self.dict())
        return out_stream.getvalue()

    @classmethod
    def from_bytes(cls, in_stream: BytesIO, schema_id: int,
                   schema: str) -> AvroSerializable:
        if schema_id not in cls._encoders:
            schema_json = json.loads(schema)
            avro_schema = fastavro.parse_schema(schema_json)
            cls._encoders[schema_id] = avro_schema
        deser_data = fastavro.schemaless_reader(in_stream,
                                                cls._encoders[schema_id])
        return cls(**deser_data)
