import re
from typing import Optional

from confluent_avro import SchemaRegistry

from kafka_streamer.topic.datatype import KafkaKey, KafkaValue

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
    ) -> KafkaValue:
        return KafkaValue(
            value_type, schema_registry=schema_registry, auto_register_schema=False
        )

    def create_key(
        self, key_type: S, schema_registry: Optional[SchemaRegistry],
    ) -> KafkaKey:
        return KafkaKey(
            key_type, schema_registry=schema_registry, auto_register_schema=False
        )

    def match(self, topic_name: str) -> bool:
        return self.pattern.match(topic_name)
