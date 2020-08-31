import asyncio
from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Set, Tuple, TypeVar, Union

import confluent_kafka
from confluent_avro import SchemaRegistry

from kafka_streamer.models import SchematicRecord, Serializable
from kafka_streamer.topic.datatype import (ByteDataType, SchematicDataType,
                                           SerializableDataType)
from kafka_streamer.utils import async_wrap, get_function_parameter_names

T = TypeVar("T", SchematicRecord, Serializable, bytes)
S = TypeVar("S", SchematicRecord, Serializable, bytes)


class BaseTopic(ABC):
    _available_parameters = ("key", "value", "offset")

    def __init__(
        self,
        topic_name: str,
        /,
        *,
        value_type: T = bytes,
        key_type: S = bytes,
        schema_registry: Optional[SchemaRegistry] = None,
    ):
        self._consumers: List[Tuple[Callable, Set[str]]] = []
        self.name = topic_name
        self.value = self.create_value(value_type, schema_registry)
        self.key = self.create_key(key_type, schema_registry)

    @property
    @abstractmethod
    def name(self):
        pass

    @name.setter
    @abstractmethod
    def name(self, topic_name: str):
        pass

    @abstractmethod
    def create_value(
        self, value_type: T, schema_registry: Optional[SchemaRegistry],
    ) -> Union[ByteDataType, SerializableDataType, SchematicDataType]:
        pass

    @abstractmethod
    def create_key(
        self, key_type: S, schema_registry: Optional[SchemaRegistry],
    ) -> Union[ByteDataType, SerializableDataType, SchematicDataType]:
        pass

    @abstractmethod
    def match(self, topic_name: str) -> bool:
        pass

    def __call__(self, func):
        async_func = async_wrap(func)
        parameters = get_function_parameter_names(func)
        self._raise_for_invalid_parameter_name(parameters)
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
            result["key"] = self.key.deserialize(msg.key())
        if "value" in parts:
            result["value"] = self.value.deserialize(msg.value())
        if "offset" in parts:
            result["offset"] = msg.offset()
        return result
