import re
import struct
from io import BytesIO
from typing import Callable, List, Optional, Union

from confluent_avro import SchemaRegistry

from .models import SchematicSerializable, Serializable


class KafkaTopic:
    _MAGIC_BYTE = 0
    _available_parameters = ("key", "value", "offset")

    def __init__(
        self,
        topic_name: str,
        produce_callback: Callable,
        value_type: Union[SchematicSerializable, Serializable, bytes] = bytes,
        key_type: Union[SchematicSerializable, Serializable, bytes] = bytes,
        schema_registry: Optional[SchemaRegistry] = None,
    ):
        self.topic_name = topic_name
        self.value_type = value_type
        self.key_type = key_type
        self._consumers: List[Callable] = []
        self._produce_callback = produce_callback
        self._registry = schema_registry

    def match(self, topic_name: str) -> bool:
        raise NotImplementedError()

    def __call__(self, func):
        async_func = utils.async_wrap(func)
        utils.raise_if_function_parameters_not_in(
            async_func, self._available_parameters
        )
        self._consumers.append((async_func, utils.get_function_parameters_name(func)))
        return async_func

    def has_consumer(self):
        return len(self._consumers) > 0

    def message_handlers(self, msg: confluent_kafka.Message):
        all_handlers = [
            consumer(**self._select_parts_of_message(msg, params))
            for consumer, params in self._consumers
        ]
        return asyncio.gather(*all_handlers)

    def _select_parts_of_message(
        self, msg: confluent_kafka.Message, parts: Tuple[str]
    ) -> dict:
        result = {}
        if "key" in parts:
            result["key"] = self.deserialize(msg.key(), self.key_type)
        if "value" in parts:
            result["value"] = self.deserialize(msg.value(), self.value_type)
        if "offset" in parts:
            result["offset"] = msg.offset()
        return result

    def deserialize(
        self, data: bytes, data_type: Union[SchematicSerializable, Serializable, bytes]
    ):
        if data_type == bytes:
            return data
        elif issubclass(data_type, SchematicSerializable):
            with BytesIO(data) as in_stream:
                magic, schema_id = struct.unpack(
                    ">bI", in_stream.read(self._registry.schema_id_size + 1)
                )
                if magic != self._MAGIC_BYTE:
                    raise TypeError("message does not start with magic byte")
                return data_type.from_bytes(
                    in_stream, schema_id, self._registry.get_schema(schema_id)
                )
        elif issubclass(data_type, Serializable):
            return data_type.from_bytes(data)


class KafkaRegexTopic(KafkaTopic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pattern = re.compile(self.topic_name)

    def match(self, topic_name: str) -> bool:
        return self.pattern.match(topic_name)


class KafkaSimpleTopic(KafkaTopic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = self.topic_name
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
            with BytesIO() as out_stream:
                out_stream.write(struct.pack("b", self._MAGIC_BYTE))
                out_stream.write(struct.pack(">I", schema_id))
                out_stream.write(data.to_bytes())
                return out_stream.getvalue()
        elif issubclass(data_type, Serializable):
            return data.to_bytes()
