from dataclasses import dataclass

@dataclass
class KafkaResponse:
    topic: str
    key: bytes = None
    value: bytes = None
