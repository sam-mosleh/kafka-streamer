from __future__ import annotations

from io import BytesIO


class Serializable:
    @staticmethod
    def from_bytes(cls, data: bytes) -> Serializable:
        raise NotImplementedError()

    def to_bytes(self):
        raise NotImplementedError()


class SchematicSerializable:
    @classmethod
    def get_model_schema(cls) -> str:
        raise NotImplementedError()

    @classmethod
    def from_bytes(
        cls, in_stream: BytesIO, schema_id: int, schema: str
    ) -> SchematicSerializable:
        raise NotImplementedError()

    def to_bytes(self) -> bytes:
        raise NotImplementedError()
