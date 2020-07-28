from abc import ABC, abstractmethod
from typing import Type, Union

from kafka_streamer.models import SchematicRecord, Serializable


class KafkaDataType(ABC):
    _MAGIC_BYTE = 0

    @abstractmethod
    def deserialize(self, data: bytes):
        pass

    @abstractmethod
    def serialize(
        self, data: Union[SchematicRecord, Type[Serializable], bytes, None]
    ) -> bytes:
        pass
