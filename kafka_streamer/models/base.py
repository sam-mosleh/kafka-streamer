from __future__ import annotations

from abc import ABCMeta, abstractmethod
from io import BytesIO


class Serializable(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
    def from_bytes(cls, data: bytes) -> Serializable:
        pass

    @abstractmethod
    def to_bytes(self):
        pass


class SchematicSerializable(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def get_model_schema(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def from_bytes(
        cls, in_stream: BytesIO, schema_id: int, schema: str
    ) -> SchematicSerializable:
        pass

    @abstractmethod
    def to_bytes(self) -> bytes:
        pass
