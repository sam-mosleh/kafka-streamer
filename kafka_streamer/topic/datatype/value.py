from .base import KafkaDataType


class KafkaValue(KafkaDataType):
    def _get_postfix(self) -> str:
        return "value"
