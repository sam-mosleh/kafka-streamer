import io
import struct
from abc import ABC, abstractmethod
from typing import Optional, TypeVar

from confluent_avro.schema_registry import CompatibilityLevel, SchemaRegistry

from kafka_streamer.models import SchematicSerializable, Serializable

T = TypeVar("T", SchematicSerializable, Serializable, bytes)


class KafkaDataType(ABC):
    _MAGIC_BYTE = 0

    def __init__(
        self,
        data_type: T,
        topic: Optional[str] = None,
        schema_registry: Optional[SchemaRegistry] = None,
        auto_register_schema: bool = True,
    ):
        self.data_type = data_type
        self.topic = topic
        self.schema_registry = schema_registry
        if auto_register_schema and issubclass(data_type, SchematicSerializable):
            self.schema_id = self.register_schema()

    def deserialize(self, data: bytes) -> T:
        if self.data_type == bytes:
            return data
        elif issubclass(self.data_type, SchematicSerializable):
            with io.BytesIO(data) as in_stream:
                return self._deserialize_schema(in_stream)
        elif issubclass(self.data_type, Serializable):
            return self.data_type.from_bytes(data)

    def _deserialize_schema(self, in_stream: io.BytesIO):
        magic, schema_id = struct.unpack(
            ">bI", in_stream.read(self.schema_registry.schema_id_size + 1)
        )
        if magic != self._MAGIC_BYTE:
            raise TypeError("message does not start with magic byte")
        return self.data_type.from_bytes(
            in_stream, schema_id, self.schema_registry.get_schema(schema_id)
        )

    def get_subject(self) -> str:
        return self.topic + "-" + self._get_postfix()

    @abstractmethod
    def _get_postfix(self) -> str:
        pass

    def register_schema(self):
        if self.topic is None or self.schema_registry is None:
            raise RuntimeError(
                "Topic and schema registry must be set in order to register the schema"
            )
        return self.schema_registry.register_schema(
            self.get_subject(), self.data_type.get_model_schema()
        )

    def serialize(self, data: Optional[T]) -> bytes:
        if data is None:
            return None
        if not isinstance(data, self.data_type):
            raise RuntimeError(f"Type of {data} does not match {self.data_type}")
        if self.data_type == bytes:
            return data
        elif issubclass(self.data_type, SchematicSerializable):
            with io.BytesIO() as out_stream:
                out_stream.write(struct.pack("b", self._MAGIC_BYTE))
                out_stream.write(struct.pack(">I", self.schema_id))
                out_stream.write(data.to_bytes())
                return out_stream.getvalue()
        elif issubclass(self.data_type, Serializable):
            return data.to_bytes()

    @property
    def compatibility(self) -> CompatibilityLevel:
        return self.schema_registry.get_subject_compatibility(self.get_subject())

    @compatibility.setter
    def compatibility(self, compatibility_level: CompatibilityLevel):
        self.schema_registry.set_subject_compatibility(
            self.get_subject(), compatibility_level
        )
