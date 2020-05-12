from __future__ import annotations

from dataclasses import dataclass


class Serializable:
    def to_bytes(self):
        raise NotImplementedError()

    @staticmethod
    def from_bytes(cls, data: bytes) -> Serializable:
        raise NotImplementedError()


class ByteSerializable(Serializable):
    def __init__(self, data):
        self.data = data

    def to_bytes(self):
        return self.data

    @classmethod
    def from_bytes(cls, data: bytes) -> ByteSerializable:
        return ByteSerializable(data)


@dataclass
class KafkaResponse:
    topic: str
    key: str = None
    value: Serializable = None
