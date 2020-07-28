import io
import json
import struct
from typing import Optional

from confluent_avro.schema_registry import CompatibilityLevel, SchemaRegistry

from kafka_streamer.exceptions import MessageDeserializationError
from kafka_streamer.models import SchematicModel, SchematicRecord

from .base import KafkaDataType


class SchematicDataType(KafkaDataType):
    def __init__(
        self,
        data_type: SchematicRecord,
        schema_registry: SchemaRegistry,
        topic: Optional[str] = None,
        subject_postfix: str = "",
    ):
        self.data_type = data_type
        self.topic = topic
        self.schema_registry = schema_registry
        self._subject_postfix = subject_postfix
        self._schema_id = None

    def get_subject(self) -> str:
        if self._subject_postfix:
            return f"{self.topic}-{self._subject_postfix}"
        else:
            return self.topic

    @property
    def schema_id(self):
        if self._schema_id is None:
            if self.topic is None or self.schema_registry is None:
                raise RuntimeError(
                    "Topic and schema registry must be set in order to register the schema"
                )
            self._schema_id = self.schema_registry.register_schema(
                self.get_subject(), json.dumps(self.data_type.schema)
            )
        return self._schema_id

    def deserialize(self, data: bytes) -> SchematicModel:
        with io.BytesIO(data) as in_stream:
            magic, schema_id = struct.unpack(
                ">bI", in_stream.read(self.schema_registry.schema_id_size + 1)
            )
            if magic != self._MAGIC_BYTE:
                raise MessageDeserializationError(
                    "message does not start with magic byte"
                )
            return self.data_type.from_bytes(
                in_stream, schema_id, self.schema_registry.get_schema(schema_id)
            )

    def serialize(self, object_model: Optional[SchematicModel]):
        if object_model:
            with io.BytesIO() as out_stream:
                out_stream.write(struct.pack("b", self._MAGIC_BYTE))
                out_stream.write(struct.pack(">I", self.schema_id))
                self.data_type.to_bytes(out_stream, object_model)
                return out_stream.getvalue()
        else:
            return None

    @property
    def compatibility(self) -> CompatibilityLevel:
        return self.schema_registry.get_subject_compatibility(self.get_subject())

    @compatibility.setter
    def compatibility(self, compatibility_level: CompatibilityLevel):
        self.schema_registry.set_subject_compatibility(
            self.get_subject(), compatibility_level
        )
