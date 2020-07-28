from __future__ import annotations

from io import BytesIO

import fastavro
from avro_schema.convertor import JsonSchema

from kafka_streamer.exceptions import MessageDeserializationError

from .base import SchematicRecord


class AvroRecord(SchematicRecord):
    def read(self, in_stream: BytesIO, schema: dict):
        try:
            return fastavro.schemaless_reader(in_stream, schema)
        except StopIteration:
            raise MessageDeserializationError(
                "The given schema might be incompatible or the data is corrupted."
            )

    def write(self, out_stream: BytesIO, object_dict: dict):
        return fastavro.schemaless_writer(out_stream, self.schema, object_dict)

    def parse(self, avro_schema: dict):
        return fastavro.parse_schema(avro_schema)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, datatype_schema: dict):
        generated_avro = JsonSchema(datatype_schema).to_avro()
        self._schema = self.parse(generated_avro)
