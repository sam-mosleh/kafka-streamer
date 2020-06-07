from .base import KafkaDataType


class KafkaValue(KafkaDataType):
    subject_postfix: str = "value"
