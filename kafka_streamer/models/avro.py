from __future__ import annotations

import json
from io import BytesIO

import fastavro
from avro_schema.convertor import JsonSchema
from pydantic import BaseModel
from pydantic.main import ModelMetaclass

from kafka_streamer.exceptions import MessageDeserializationError

from .base import SchematicSerializable


class AvroSchemaMetaClass(ModelMetaclass):
    def __new__(cls, name, bases, dct):
        new_class = super().__new__(cls, name, bases, dct)
        generated_avro = JsonSchema(new_class.schema()).to_avro()
        new_class._avro_schema = fastavro.parse_schema(generated_avro)
        return new_class


class AvroRecord(SchematicSerializable, BaseModel, metaclass=AvroSchemaMetaClass):
    _encoders = {}
    _avro_schema: dict = {}

    @classmethod
    def get_model_schema(cls) -> str:
        return json.dumps(JsonSchema(cls.schema()).to_avro())

    @classmethod
    def from_bytes(cls, in_stream: BytesIO, schema_id: int, schema: str) -> AvroRecord:
        if schema_id not in AvroRecord._encoders:
            schema_json = json.loads(schema)
            avro_schema = fastavro.parse_schema(schema_json)
            AvroRecord._encoders[schema_id] = avro_schema
        try:
            deser_data = fastavro.schemaless_reader(
                in_stream, AvroRecord._encoders[schema_id]
            )
        except StopIteration:
            raise MessageDeserializationError(
                "The given schema might be incompatible or the data is corrupted."
            )
        return cls(**deser_data)

    def to_bytes(self) -> bytes:
        with BytesIO() as out_stream:
            fastavro.schemaless_writer(out_stream, self._avro_schema, self.dict())
            return out_stream.getvalue()
