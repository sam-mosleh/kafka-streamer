from typing import Optional, Type

from kafka_streamer.models import Serializable

from .base import KafkaDataType


class SerializableDataType(KafkaDataType):
    def __init__(self, data_type: Type[Serializable]):
        self.data_type = data_type

    def deserialize(self, data: bytes) -> Serializable:
        return self.data_type.from_bytes(data)

    def serialize(self, object_model: Optional[Serializable]):
        if object_model:
            return object_model.to_bytes()
        else:
            return None
