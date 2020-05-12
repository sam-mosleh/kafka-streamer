from __future__ import annotations

from dataclasses import dataclass


class SerializableObject:
    def to_bytes(self):
        raise NotImplementedError()

    @staticmethod
    def from_bytes(cls, data: bytes) -> SerializableObject:
        raise NotImplementedError()


@dataclass
class KafkaResponse:
    topic: str
    key: str = None
    value: SerializableObject = None
