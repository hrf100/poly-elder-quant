"""Fast JSON serialization/deserialization using orjson when installed, with seamless stdlib fallback."""
from typing import Any

try:
    import orjson as _orjson

    def loads(data: str | bytes | bytearray) -> Any:
        return _orjson.loads(data)

    def dumps_str(obj: Any) -> str:
        return _orjson.dumps(obj).decode("utf-8")

    HAS_ORJSON = True
except Exception:
    import json as _json

    def loads(data: str | bytes | bytearray) -> Any:
        return _json.loads(data)

    def dumps_str(obj: Any) -> str:
        return _json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    HAS_ORJSON = False
