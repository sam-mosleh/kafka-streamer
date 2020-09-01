__version__ = "0.4.7"

from confluent_avro.schema_registry import CompatibilityLevel

from .streamer import KafkaStreamer

__all__ = ("KafkaStreamer", "CompatibilityLevel")
