import re
from typing import Optional, Union

from confluent_avro import SchemaRegistry

from kafka_streamer.topic.datatype import (ByteDataType, SchematicDataType,
                                           SerializableDataType,
                                           keytype_selector,
                                           valuetype_selector)

from .base import BaseTopic, S, T


class RegexTopic(BaseTopic):
    @property
    def name(self):
        return self._pattern.pattern

    @name.setter
    def name(self, topic_name: str):
        self._pattern = re.compile(topic_name)

    def create_value(
        self, value_type: T, schema_registry: Optional[SchemaRegistry],
    ) -> Union[ByteDataType, SerializableDataType, SchematicDataType]:
        return valuetype_selector(value_type, schema_registry=schema_registry,)

    def create_key(
        self, key_type: S, schema_registry: Optional[SchemaRegistry],
    ) -> Union[ByteDataType, SerializableDataType, SchematicDataType]:
        return keytype_selector(key_type, schema_registry=schema_registry,)

    def match(self, topic_name: str) -> bool:
        return self._pattern.match(topic_name)
