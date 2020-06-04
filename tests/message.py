from typing import Optional


class SampleMessage:
    def __init__(
        self,
        *,
        value: bytes = b"",
        key: Optional[bytes] = None,
        offset: int = 0,
        error: str = ""
    ):
        self._value = value
        self._key = key
        self._offset = offset
        self._error = error

    def value(self):
        return self._value

    def key(self):
        return self._key

    def offset(self):
        return self._offset

    def error(self):
        return self._error
