"""
unity_msgpack_avro – decode/encode compact Unity MessagePack using exported Avro schemas.

Workflow:
  1. Export Avro schema from the C# tool:
       dotnet cli.dll avro MyClass --dll Assembly.dll > schema.json
  2. Load schema and decode compact msgpack:
       from unity_msgpack_avro import load_schema, decode, encode
       schema = load_schema("schema.json")
       obj = decode(schema, compact_bytes)

The library handles:
  • msgpack_key  – int or string key per field
  • msgpack_key_type – map whose keys are non-string (stored as JSON-string in Avro)
  • msgpack_unions – [discriminator, payload] union dispatch

Decoded values are Python dicts / lists / primitives.
Encode accepts the same structure and returns compact msgpack bytes.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Any

import msgspec.msgpack as _mp

__all__ = [
    "Schema",
    "Registry",
    "load_schema",
    "load_bytes",
    "decode",
    "encode",
]


# ──────────────────────────────────────────────────────────────────────────────
# Schema model
# ──────────────────────────────────────────────────────────────────────────────

class Schema:
    """Parsed Avro schema node enriched with msgpack_* custom fields."""

    __slots__ = (
        "type", "name", "fields", "items", "values",
        "key_type", "union_of", "union_disp", "_registry",
    )

    def __init__(self, type_: str, name: str = "") -> None:
        self.type: str = type_          # "record","array","map","union","null","int",…
        self.name: str = name           # fully-qualified for records
        self.fields: list[Field] = []   # record fields
        self.items: Schema | None = None   # array element type
        self.values: Schema | None = None  # map value type
        self.key_type: str = ""            # non-string map key type (msgpack_key_type)
        self.union_of: list[Schema] = []   # Avro union variants
        self.union_disp: list[UnionVariant] = []  # msgpack_unions dispatch table
        self._registry: Registry = {}

    def resolve(self) -> Schema:
        """Follow ref schemas to their real definition."""
        if self.type == "ref":
            return self._registry.get(self.name, self)
        return self

    def __repr__(self) -> str:
        if self.name:
            return f"Schema({self.type!r}, {self.name!r})"
        return f"Schema({self.type!r})"


class Field:
    """A record field with its msgpack key."""
    __slots__ = ("name", "type", "msgpack_key")

    def __init__(self, name: str, type_: Schema, msgpack_key: int | str) -> None:
        self.name = name
        self.type = type_
        self.msgpack_key: int | str = msgpack_key


class UnionVariant:
    """One entry in msgpack_unions."""
    __slots__ = ("key", "type_name")

    def __init__(self, key: int, type_name: str) -> None:
        self.key = key
        self.type_name = type_name


Registry = dict[str, Schema]


# ──────────────────────────────────────────────────────────────────────────────
# Loading
# ──────────────────────────────────────────────────────────────────────────────

def load_schema(path: str | Path) -> Schema:
    """Load the *first* schema from a file; also populates the registry."""
    _, root = load_bytes(Path(path).read_bytes())
    return root


def load_registry(path: str | Path) -> Registry:
    """Load all schemas from a file and return the registry."""
    reg, _ = load_bytes(Path(path).read_bytes())
    return reg


def load_bytes(data: bytes) -> tuple[Registry, Schema]:
    """Parse Avro schema JSON bytes; returns (registry, root_schema)."""
    raw = json.loads(data)
    reg: Registry = {}

    if isinstance(raw, list):
        root = None
        for item in raw:
            s = _parse_schema(item, reg)
            if root is None:
                root = s
    else:
        root = _parse_schema(raw, reg)

    _patch_registry(root, reg)
    for s in list(reg.values()):
        _patch_registry(s, reg)

    return reg, root  # type: ignore[return-value]


def _patch_registry(s: Schema | None, reg: Registry) -> None:
    if s is None:
        return
    s._registry = reg
    for f in s.fields:
        _patch_registry(f.type, reg)
    _patch_registry(s.items, reg)
    _patch_registry(s.values, reg)
    for u in s.union_of:
        _patch_registry(u, reg)


# ──────────────────────────────────────────────────────────────────────────────
# JSON -> Schema parser
# ──────────────────────────────────────────────────────────────────────────────

_PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}


def _parse_schema(raw: Any, reg: Registry) -> Schema:
    if raw is None:
        return Schema("null")
    if isinstance(raw, str):
        return _primitive_or_ref(raw, reg)
    if isinstance(raw, list):
        return _parse_union_array(raw, reg)
    if isinstance(raw, dict):
        return _parse_object(raw, reg)
    return Schema("null")


def _primitive_or_ref(name: str, reg: Registry) -> Schema:
    if name in _PRIMITIVES:
        return Schema(name)
    if name in reg:
        return reg[name]
    s = Schema("ref", name)
    return s


def _parse_union_array(items: list, reg: Registry) -> Schema:
    s = Schema("union")
    for item in items:
        s.union_of.append(_parse_schema(item, reg))
    return s


def _parse_object(obj: dict, reg: Registry) -> Schema:
    type_raw = obj.get("type")
    if type_raw is None:
        return Schema("null")
    if isinstance(type_raw, list):
        return _parse_union_array(type_raw, reg)
    if isinstance(type_raw, str):
        if type_raw == "record":
            return _parse_record(obj, reg)
        if type_raw == "array":
            return _parse_array(obj, reg)
        if type_raw == "map":
            return _parse_map(obj, reg)
        return _primitive_or_ref(type_raw, reg)
    return Schema("null")


def _parse_record(obj: dict, reg: Registry) -> Schema:
    name = obj.get("name", "")
    ns = obj.get("namespace", "")
    if ns and "." not in name:
        fqn = f"{ns}.{name}"
    else:
        fqn = name

    s = Schema("record", fqn)

    # msgpack_unions
    for u in obj.get("msgpack_unions", []):
        s.union_disp.append(UnionVariant(u["key"], u["type"]))

    # fields
    for rf in obj.get("fields", []):
        f = _parse_field(rf, reg)
        s.fields.append(f)

    # Register both FQN and short name.
    reg[fqn] = s
    short = fqn.rsplit(".", 1)[-1]
    reg[short] = s
    return s


def _parse_field(obj: dict, reg: Registry) -> Field:
    name = obj.get("name", "")
    type_schema = _parse_schema(obj.get("type"), reg)

    key_raw = obj.get("msgpack_key", name)
    msgpack_key: int | str = key_raw  # already int or str from JSON

    return Field(name, type_schema, msgpack_key)


def _parse_array(obj: dict, reg: Registry) -> Schema:
    s = Schema("array")
    if "items" in obj:
        s.items = _parse_schema(obj["items"], reg)
    return s


def _parse_map(obj: dict, reg: Registry) -> Schema:
    s = Schema("map")
    if "values" in obj:
        s.values = _parse_schema(obj["values"], reg)
    s.key_type = obj.get("msgpack_key_type", "")
    return s


# ──────────────────────────────────────────────────────────────────────────────
# Decoder: compact msgpack bytes -> Python value
# ──────────────────────────────────────────────────────────────────────────────

def decode(schema: Schema, data: bytes) -> Any:
    """
    Decode compact msgpack *data* according to *schema*.

    Returns Python primitives / dicts / lists.
    Records are returned as ``dict[str, Any]`` keyed by field **name**.
    Union-dispatch records are ``{"__type": int, "value": Any}``.
    Maps with non-string keys (msgpack_key_type) are ``dict[Any, Any]``.
    """
    unpacker = _Unpacker(data)
    return _decode_value(schema, unpacker)


class _Unpacker:
    """Thin wrapper around msgspec.msgpack raw decoder."""

    def __init__(self, data: bytes) -> None:
        self._data = memoryview(data)
        self._pos = 0

    def peek(self) -> int:
        return self._data[self._pos]

    def read_raw(self) -> Any:
        """Decode one value at current position, advancing pointer."""
        dec = _mp.Decoder()
        # msgspec doesn't expose streaming, so we use struct-based peeking.
        # Fall back to full decode via msgpack-python for streaming.
        raise NotImplementedError("use _decode_value directly")

    # ── Low-level helpers ──────────────────────────────────────────────────

    def _byte(self) -> int:
        b = self._data[self._pos]
        self._pos += 1
        return b

    def _read(self, n: int) -> bytes:
        out = bytes(self._data[self._pos:self._pos + n])
        self._pos += n
        return out

    # ── Unsigned integer ──────────────────────────────────────────────────

    def read_uint(self) -> int:
        b = self._byte()
        if b <= 0x7F:           # positive fixint
            return b
        if b == 0xCC:           # uint8
            return self._byte()
        if b == 0xCD:           # uint16
            return struct.unpack_from(">H", self._read(2))[0]
        if b == 0xCE:           # uint32
            return struct.unpack_from(">I", self._read(4))[0]
        if b == 0xCF:           # uint64
            return struct.unpack_from(">Q", self._read(8))[0]
        if 0xE0 <= b <= 0xFF:   # negative fixint
            return struct.unpack("b", bytes([b]))[0]
        if b == 0xD0:
            return struct.unpack("b", self._read(1))[0]
        if b == 0xD1:
            return struct.unpack(">h", self._read(2))[0]
        if b == 0xD2:
            return struct.unpack(">i", self._read(4))[0]
        if b == 0xD3:
            return struct.unpack(">q", self._read(8))[0]
        raise ValueError(f"Expected int/uint, got 0x{b:02X} at pos {self._pos-1}")

    # ── Array length ──────────────────────────────────────────────────────

    def read_array_len(self) -> int | None:
        b = self._byte()
        if b == 0xC0:  # nil
            return None
        if 0x90 <= b <= 0x9F:   # fixarray
            return b & 0x0F
        if b == 0xDC:           # array16
            return struct.unpack_from(">H", self._read(2))[0]
        if b == 0xDD:           # array32
            return struct.unpack_from(">I", self._read(4))[0]
        raise ValueError(f"Expected array, got 0x{b:02X} at pos {self._pos-1}")

    # ── Map length ────────────────────────────────────────────────────────

    def read_map_len(self) -> int | None:
        b = self._byte()
        if b == 0xC0:  # nil
            return None
        if 0x80 <= b <= 0x8F:   # fixmap
            return b & 0x0F
        if b == 0xDE:           # map16
            return struct.unpack_from(">H", self._read(2))[0]
        if b == 0xDF:           # map32
            return struct.unpack_from(">I", self._read(4))[0]
        raise ValueError(f"Expected map, got 0x{b:02X} at pos {self._pos-1}")

    # ── Generic value (for skipping / raw decode) ─────────────────────────

    def read_any(self) -> Any:
        """Decode one msgpack value generically."""
        b = self._data[self._pos]
        # Use msgspec for the full value decode at current position.
        # Since msgspec doesn't support streaming, decode from current slice
        # and skip the consumed bytes manually by tracking position.
        # Workaround: decode entire remaining slice and measure offset via trial.
        # Better: use msgpack-python which does support streaming.
        import msgpack  # optional dependency, used only for generic skip
        unpacked, new_pos = msgpack.unpackb(
            bytes(self._data[self._pos:]),
            raw=False,
            strict_map_key=False,
        ), None
        # msgpack.unpackb doesn't report bytes consumed; use Unpacker.
        unpkr = msgpack.Unpacker(raw=False, strict_map_key=False)
        unpkr.feed(bytes(self._data[self._pos:]))
        val = unpkr.unpack()
        consumed = unpkr.tell()
        self._pos += consumed
        return val

    def remaining(self) -> bytes:
        return bytes(self._data[self._pos:])


def _decode_value(schema: Schema | None, unpkr: _Unpacker) -> Any:
    if schema is None:
        return unpkr.read_any()
    schema = schema.resolve()

    t = schema.type
    if t == "null":
        b = unpkr._byte()
        if b != 0xC0:
            raise ValueError(f"Expected nil (0xC0), got 0x{b:02X}")
        return None

    if t == "boolean":
        b = unpkr._byte()
        if b == 0xC3:
            return True
        if b == 0xC2:
            return False
        raise ValueError(f"Expected bool, got 0x{b:02X}")

    if t in ("int", "long"):
        return unpkr.read_uint()

    if t in ("float", "double"):
        b = unpkr._byte()
        if b == 0xCA:  # float32
            return struct.unpack(">f", unpkr._read(4))[0]
        if b == 0xCB:  # float64
            return struct.unpack(">d", unpkr._read(8))[0]
        # Fallback: might be encoded as int
        unpkr._pos -= 1
        return float(unpkr.read_uint())

    if t == "string":
        return _read_str(unpkr)

    if t == "bytes":
        return _read_bytes(unpkr)

    if t == "record":
        return _decode_record(schema, unpkr)

    if t == "array":
        return _decode_array(schema, unpkr)

    if t == "map":
        return _decode_map(schema, unpkr)

    if t == "union":
        return _decode_union(schema, unpkr)

    # Unknown / primitive fallback.
    return unpkr.read_any()


def _read_str(unpkr: _Unpacker) -> str:
    b = unpkr._byte()
    if b == 0xC0:
        return None  # type: ignore[return-value]
    if (b & 0xE0) == 0xA0:  # fixstr
        length = b & 0x1F
    elif b == 0xD9:
        length = unpkr._byte()
    elif b == 0xDA:
        length = struct.unpack_from(">H", unpkr._read(2))[0]
    elif b == 0xDB:
        length = struct.unpack_from(">I", unpkr._read(4))[0]
    else:
        raise ValueError(f"Expected str, got 0x{b:02X} at {unpkr._pos-1}")
    return unpkr._read(length).decode("utf-8")


def _read_bytes(unpkr: _Unpacker) -> bytes:
    b = unpkr._byte()
    if b == 0xC0:
        return None  # type: ignore[return-value]
    if b == 0xC4:
        length = unpkr._byte()
    elif b == 0xC5:
        length = struct.unpack_from(">H", unpkr._read(2))[0]
    elif b == 0xC6:
        length = struct.unpack_from(">I", unpkr._read(4))[0]
    else:
        raise ValueError(f"Expected bin, got 0x{b:02X} at {unpkr._pos-1}")
    return unpkr._read(length)


def _is_array_code(b: int) -> bool:
    return (0x90 <= b <= 0x9F) or b in (0xDC, 0xDD)


def _is_nil_code(b: int) -> bool:
    return b == 0xC0


def _decode_record(schema: Schema, unpkr: _Unpacker) -> Any:
    if schema.union_disp:
        return _decode_union_dispatch(schema, unpkr)

    b = unpkr.peek()
    if _is_nil_code(b):
        unpkr._byte()
        return None
    if _is_array_code(b):
        return _decode_int_keyed_record(schema, unpkr)
    return _decode_str_keyed_record(schema, unpkr)


def _decode_int_keyed_record(schema: Schema, unpkr: _Unpacker) -> dict:
    length = unpkr.read_array_len()
    if length is None:
        return None  # type: ignore[return-value]
    by_index: dict[int, Field] = {f.msgpack_key: f for f in schema.fields if isinstance(f.msgpack_key, int)}
    result: dict[str, Any] = {}
    for i in range(length):
        f = by_index.get(i)
        if f is not None:
            result[f.name] = _decode_value(f.type, unpkr)
        else:
            unpkr.read_any()
    return result


def _decode_str_keyed_record(schema: Schema, unpkr: _Unpacker) -> dict:
    length = unpkr.read_map_len()
    if length is None:
        return None  # type: ignore[return-value]
    by_key: dict[str, Field] = {}
    for f in schema.fields:
        k = f.msgpack_key
        by_key[str(k)] = f

    result: dict[str, Any] = {}
    for _ in range(length):
        key = _read_str(unpkr)
        f = by_key.get(key)
        if f is not None:
            result[f.name] = _decode_value(f.type, unpkr)
        else:
            unpkr.read_any()
    return result


def _decode_union_dispatch(schema: Schema, unpkr: _Unpacker) -> dict:
    length = unpkr.read_array_len()
    if length is None:
        return None  # type: ignore[return-value]
    if length != 2:
        raise ValueError(f"Union dispatch: expected [disc, payload], got len={length}")
    discriminator = unpkr.read_uint()

    variant_schema: Schema | None = None
    for v in schema.union_disp:
        if v.key == discriminator:
            variant_schema = schema._registry.get(v.type_name)
            break

    payload = _decode_value(variant_schema, unpkr)
    return {"__type": discriminator, "value": payload}


def _decode_array(schema: Schema, unpkr: _Unpacker) -> list:
    length = unpkr.read_array_len()
    if length is None:
        return None  # type: ignore[return-value]
    return [_decode_value(schema.items, unpkr) for _ in range(length)]


def _decode_map(schema: Schema, unpkr: _Unpacker) -> dict:
    length = unpkr.read_map_len()
    if length is None:
        return None  # type: ignore[return-value]

    result: dict[Any, Any] = {}
    for _ in range(length):
        raw_key = _read_str(unpkr)
        value = _decode_value(schema.values, unpkr)
        if schema.key_type:
            native_key = _parse_key_type(raw_key, schema.key_type)
        else:
            native_key = raw_key
        result[native_key] = value
    return result


def _decode_union(schema: Schema, unpkr: _Unpacker) -> Any:
    b = unpkr.peek()
    if _is_nil_code(b):
        unpkr._byte()
        return None
    for variant in schema.union_of:
        variant = variant.resolve()
        if variant.type != "null":
            return _decode_value(variant, unpkr)
    return unpkr.read_any()


# ──────────────────────────────────────────────────────────────────────────────
# Encoder: Python value -> compact msgpack bytes
# ──────────────────────────────────────────────────────────────────────────────

def encode(schema: Schema, value: Any) -> bytes:
    """
    Encode a Python value into compact msgpack bytes according to *schema*.
    The input should mirror the structure returned by :func:`decode`.
    """
    buf: list[bytes] = []
    _encode_value(schema, value, buf)
    return b"".join(buf)


def _encode_value(schema: Schema | None, value: Any, buf: list[bytes]) -> None:
    if schema is None:
        buf.append(_mp.encode(value))
        return
    schema = schema.resolve()
    t = schema.type

    if value is None or t == "null":
        buf.append(b"\xC0")
        return

    if t == "boolean":
        buf.append(b"\xC3" if value else b"\xC2")
        return

    if t in ("int", "long"):
        buf.append(_encode_int(int(value)))
        return

    if t == "float":
        buf.append(b"\xCA" + struct.pack(">f", float(value)))
        return

    if t == "double":
        buf.append(b"\xCB" + struct.pack(">d", float(value)))
        return

    if t == "string":
        buf.append(_encode_str(str(value)))
        return

    if t == "bytes":
        buf.append(_encode_bytes(bytes(value)))
        return

    if t == "record":
        _encode_record(schema, value, buf)
        return

    if t == "array":
        _encode_array(schema, value, buf)
        return

    if t == "map":
        _encode_map(schema, value, buf)
        return

    if t == "union":
        _encode_union(schema, value, buf)
        return

    buf.append(_mp.encode(value))


def _encode_record(schema: Schema, value: dict, buf: list[bytes]) -> None:
    if not isinstance(value, dict):
        buf.append(b"\xC0")
        return

    # Union dispatch
    if schema.union_disp:
        discriminator = int(value["__type"])
        payload = value.get("value")
        variant_schema: Schema | None = None
        for v in schema.union_disp:
            if v.key == discriminator:
                variant_schema = schema._registry.get(v.type_name)
                break
        buf.append(_encode_array_header(2))
        buf.append(_encode_int(discriminator))
        _encode_value(variant_schema, payload, buf)
        return

    # Check if int-keyed
    int_keyed = any(isinstance(f.msgpack_key, int) for f in schema.fields)
    if int_keyed:
        max_idx = max(f.msgpack_key for f in schema.fields if isinstance(f.msgpack_key, int))
        by_idx = {f.msgpack_key: f for f in schema.fields if isinstance(f.msgpack_key, int)}
        buf.append(_encode_array_header(max_idx + 1))
        for i in range(max_idx + 1):
            f = by_idx.get(i)
            if f is not None:
                _encode_value(f.type, value.get(f.name), buf)
            else:
                buf.append(b"\xC0")
    else:
        buf.append(_encode_map_header(len(schema.fields)))
        for f in schema.fields:
            key = f.msgpack_key if isinstance(f.msgpack_key, str) else f.name
            buf.append(_encode_str(key))
            _encode_value(f.type, value.get(f.name), buf)


def _encode_array(schema: Schema, value: list, buf: list[bytes]) -> None:
    if not isinstance(value, (list, tuple)):
        buf.append(b"\xC0")
        return
    buf.append(_encode_array_header(len(value)))
    for item in value:
        _encode_value(schema.items, item, buf)


def _encode_map(schema: Schema, value: dict, buf: list[bytes]) -> None:
    if not isinstance(value, dict):
        buf.append(b"\xC0")
        return
    buf.append(_encode_map_header(len(value)))
    for k, v in value.items():
        if schema.key_type:
            buf.append(_encode_str(_stringify_key(k, schema.key_type)))
        else:
            buf.append(_encode_str(str(k)))
        _encode_value(schema.values, v, buf)


def _encode_union(schema: Schema, value: Any, buf: list[bytes]) -> None:
    if value is None:
        buf.append(b"\xC0")
        return
    for variant in schema.union_of:
        variant = variant.resolve()
        if variant.type != "null":
            _encode_value(variant, value, buf)
            return
    buf.append(_mp.encode(value))


# ──────────────────────────────────────────────────────────────────────────────
# Low-level msgpack writers
# ──────────────────────────────────────────────────────────────────────────────

def _encode_int(n: int) -> bytes:
    if 0 <= n <= 0x7F:
        return bytes([n])
    if -32 <= n < 0:
        return bytes([n & 0xFF])
    if 0 <= n <= 0xFF:
        return b"\xCC" + bytes([n])
    if 0 <= n <= 0xFFFF:
        return b"\xCD" + struct.pack(">H", n)
    if 0 <= n <= 0xFFFFFFFF:
        return b"\xCE" + struct.pack(">I", n)
    if 0 <= n <= 0xFFFFFFFFFFFFFFFF:
        return b"\xCF" + struct.pack(">Q", n)
    if -0x80 <= n < 0:
        return b"\xD0" + struct.pack("b", n)
    if -0x8000 <= n < 0:
        return b"\xD1" + struct.pack(">h", n)
    if -0x80000000 <= n < 0:
        return b"\xD2" + struct.pack(">i", n)
    return b"\xD3" + struct.pack(">q", n)


def _encode_str(s: str) -> bytes:
    encoded = s.encode("utf-8")
    n = len(encoded)
    if n <= 31:
        return bytes([0xA0 | n]) + encoded
    if n <= 0xFF:
        return b"\xD9" + bytes([n]) + encoded
    if n <= 0xFFFF:
        return b"\xDA" + struct.pack(">H", n) + encoded
    return b"\xDB" + struct.pack(">I", n) + encoded


def _encode_bytes(b: bytes) -> bytes:
    n = len(b)
    if n <= 0xFF:
        return b"\xC4" + bytes([n]) + b
    if n <= 0xFFFF:
        return b"\xC5" + struct.pack(">H", n) + b
    return b"\xC6" + struct.pack(">I", n) + b


def _encode_array_header(n: int) -> bytes:
    if n <= 15:
        return bytes([0x90 | n])
    if n <= 0xFFFF:
        return b"\xDC" + struct.pack(">H", n)
    return b"\xDD" + struct.pack(">I", n)


def _encode_map_header(n: int) -> bytes:
    if n <= 15:
        return bytes([0x80 | n])
    if n <= 0xFFFF:
        return b"\xDE" + struct.pack(">H", n)
    return b"\xDF" + struct.pack(">I", n)


# ──────────────────────────────────────────────────────────────────────────────
# Key type helpers
# ──────────────────────────────────────────────────────────────────────────────

def _parse_key_type(s: str, key_type: str) -> Any:
    """Convert a JSON-stringified map key back to its native Python type."""
    if key_type in ("int", "long"):
        return int(s)
    if key_type == "float":
        return struct.unpack(">f", struct.pack(">f", float(s)))[0]  # float32
    if key_type == "double":
        return float(s)
    if key_type == "boolean":
        return s.lower() == "true"
    if key_type == "bytes":
        return _unescape_bytes(s)
    return s


def _stringify_key(k: Any, key_type: str) -> str:
    """Convert a native key to its JSON-string form for msgpack storage."""
    if isinstance(k, bool):
        return str(k).lower()
    if isinstance(k, int):
        return str(k)
    if isinstance(k, float):
        return repr(k)
    if isinstance(k, (bytes, bytearray)):
        return _escape_bytes(k)
    return str(k)


def _escape_bytes(b: bytes | bytearray) -> str:
    return "".join(f"\\u{c:04X}" for c in b)


def _unescape_bytes(s: str) -> bytes:
    out = bytearray()
    i = 0
    while i < len(s):
        if s[i] == "\\" and i + 5 <= len(s) and s[i + 1] == "u":
            out.append(int(s[i + 2:i + 6], 16))
            i += 6
        else:
            out.append(ord(s[i]))
            i += 1
    return bytes(out)
