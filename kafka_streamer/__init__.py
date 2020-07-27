__version__ = "0.4.0"

from confluent_avro.schema_registry import CompatibilityLevel

from .streamer import KafkaStreamer

__all__ = ("KafkaStreamer", "CompatibilityLevel")
