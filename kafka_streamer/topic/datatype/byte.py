from typing import Optional

from .base import KafkaDataType


class ByteDataType(KafkaDataType):
    def deserialize(self, data: bytes) -> bytes:
        return data

    def serialize(self, data: Optional[bytes]):
        return data
