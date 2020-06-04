import asyncio
import io
import struct
from typing import Callable, List, Optional, Set, Tuple, Union

import confluent_kafka
from confluent_avro import SchemaRegistry

from kafka_streamer.models import SchematicSerializable, Serializable
from kafka_streamer.utils import async_wrap, get_function_parameter_names


class BaseTopic:
    _MAGIC_BYTE = 0
    _available_parameters = ("key", "value", "offset")

    def __init__(
        self,
        topic_name: str,
        *,
        value_type: Union[SchematicSerializable, Serializable, bytes] = bytes,
        key_type: Union[SchematicSerializable, Serializable, bytes] = bytes,
        schema_registry: Optional[SchemaRegistry] = None,
    ):
        self.topic_name = topic_name
        self.value_type = value_type
        self.key_type = key_type
        self._consumers: List[Tuple[Callable, Set[str]]] = []
        self._registry = schema_registry

    def match(self, topic_name: str) -> bool:
        raise NotImplementedError()

    def __call__(self, func):
        async_func = async_wrap(func)
        parameters = get_function_parameter_names(func)
        self._raise_for_invalid_parameter_name(parameters)
        print("Async:", async_func)
        self._add(async_func, parameters)
        return async_func

    def _add(self, func: Callable, parameters: Set[str]):
        self._consumers.append((func, parameters))

    def _raise_for_invalid_parameter_name(self, parameters: Set[str]):
        if len(parameters) == 0:
            raise TypeError("Cannot have consumer without parameters")
        for parameter in parameters:
            if parameter not in self._available_parameters:
                raise TypeError(
                    f"{parameter} is not a valid function parameter."
                    f"Available parameters: {self._available_parameters}"
                )

    def has_consumer(self):
        return len(self._consumers) > 0

    def message_handlers(self, msg: confluent_kafka.Message):
        all_handlers = [
            consumer(**self._select_parts_of_message(msg, params))
            for consumer, params in self._consumers
        ]
        return asyncio.gather(*all_handlers)

    def _select_parts_of_message(
        self, msg: confluent_kafka.Message, parts: Set[str]
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
            with io.BytesIO(data) as in_stream:
                return self._deserialize_schema(in_stream, data_type)
        elif issubclass(data_type, Serializable):
            return data_type.from_bytes(data)

    def _deserialize_schema(
        self,
        in_stream: io.BytesIO,
        data_type: Union[SchematicSerializable, Serializable, bytes],
    ):
        magic, schema_id = struct.unpack(
            ">bI", in_stream.read(self._registry.schema_id_size + 1)
        )
        if magic != self._MAGIC_BYTE:
            raise TypeError("message does not start with magic byte")
        return data_type.from_bytes(
            in_stream, schema_id, self._registry.get_schema(schema_id)
        )
