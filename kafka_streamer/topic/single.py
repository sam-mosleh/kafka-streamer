from typing import Callable, Optional

from confluent_avro import SchemaRegistry

from kafka_streamer.topic.datatype import KafkaKey, KafkaValue

from .base import BaseTopic, S, T


class SingleTopic(BaseTopic):
    def __init__(self, *args, produce_callback: Callable, **kwargs):
        super().__init__(*args, **kwargs)
        self._produce_callback = produce_callback

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, topic_name: str):
        self._name = topic_name

    def create_value(
        self, value_type: T, schema_registry: Optional[SchemaRegistry],
    ) -> KafkaValue:
        return KafkaValue(value_type, self.name, schema_registry=schema_registry)

    def create_key(
        self, key_type: S, schema_registry: Optional[SchemaRegistry],
    ) -> KafkaKey:
        return KafkaKey(key_type, self.name, schema_registry=schema_registry)

    def match(self, topic_name: str) -> bool:
        return self.name == topic_name

    async def send(self, value: T, key: Optional[S] = None):
        await self._produce_callback(
            topic=self.name,
            value=self.value.serialize(value),
            key=self.key.serialize(key),
        )
