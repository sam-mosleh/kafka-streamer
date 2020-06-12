__version__ = "0.3.1"

from confluent_avro.schema_registry import CompatibilityLevel

from .streamer import KafkaStreamer

__all__ = ("KafkaStreamer", "CompatibilityLevel")
