import io
import struct
from typing import Callable, Union

from kafka_streamer.models import SchematicSerializable, Serializable

from .base import BaseTopic


class SingleTopic(BaseTopic):
    def __init__(self, *args, produce_callback: Callable, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = self.topic_name
        self._produce_callback = produce_callback
        if issubclass(self.key_type, SchematicSerializable):
            self.key_schema_id = self.register_schema("key", self.key_type)
        if issubclass(self.value_type, SchematicSerializable):
            self.value_schema_id = self.register_schema("value", self.value_type)

    def match(self, topic_name: str) -> bool:
        return self.name == topic_name

    def get_subject(self, postfix: str):
        return f"{self.name}-{postfix}"

    def register_schema(self, postfix: str, data_type: SchematicSerializable):
        if self._registry is None:
            raise RuntimeError(f"Registry needs to be specified for type: {data_type}")
        return self._registry.register_schema(
            self.get_subject(postfix), data_type.get_model_schema()
        )

    async def send(self, key, value):
        await self._produce_callback(
            topic=self.name,
            value=self.serialize(value, False),
            key=self.serialize(key, True),
        )

    def serialize(
        self, data: Union[SchematicSerializable, Serializable, bytes], is_key: bool
    ):
        data_type = self.key_type if is_key else self.value_type
        if not isinstance(data, data_type):
            raise RuntimeError(f"Data type {data} does not match {data_type}")
        if data_type == bytes:
            return data
        elif issubclass(data_type, SchematicSerializable):
            schema_id = self.key_schema_id if is_key else self.value_schema_id
            with io.BytesIO() as out_stream:
                out_stream.write(struct.pack("b", self._MAGIC_BYTE))
                out_stream.write(struct.pack(">I", schema_id))
                out_stream.write(data.to_bytes())
                return out_stream.getvalue()
        elif issubclass(data_type, Serializable):
            return data.to_bytes()
