from .base import KafkaDataType
from typing import Optional


class ByteDataType(KafkaDataType):
    def deserialize(self, data: bytes) -> bytes:
        return data

    def serialize(self, data: Optional[bytes]):
        return data
