# unity-msgpack-schema-pybind

Python bindings for Unity MsgPack Schema Tools via NativeAOT FFI.

## Quick Start

```python
import msgpack
from unity_msgpack_schema import MsgPackSchema

with MsgPackSchema("/path/to/DummyDll") as schema:
    # decode(): compact indexed msgpack  ->  named-key msgpack map
    named_bytes = schema.decode("MyMsgPackClass", compact_bytes)
    obj = msgpack.unpackb(named_bytes, raw=False)   # {'id': 1, 'name': 'foo', ...}

    # encode(): named-key msgpack map  ->  compact indexed msgpack
    compact = schema.encode("MyMsgPackClass", msgpack.packb({'id': 1, 'name': 'foo'}))

    # decode_json(): compact msgpack  ->  Python dict  (goes through JSON internally)
    decoded = schema.decode_json("MyMsgPackClass", compact_bytes)

    # encode_json(): Python dict  ->  compact msgpack  (goes through JSON internally)
    compact = schema.encode_json("MyMsgPackClass", {'id': 1, 'name': 'foo'})

    # Schema introspection 
    classes = schema.list_classes()                    # list[str]
    info    = schema.get_schema("MyMsgPackClass")      # dict

    # Avro export
    avro = schema.avro_schema("MyMsgPackClass")                    # basic
    avro = schema.avro_schema("MyMsgPackClass", nullable=True)     # all fields nullable
    avro = schema.avro_schema("MyMsgPackClass", flat=True)         # referenced, not inlined
```

See a full script in [example](./example/lib_example.py).

## Usage

### `MsgPackSchema(dll_path, *, lib_path=None)`

| Method | Input -> Output | Description |
|---|---|---|
| `decode(cls_name, bytes)` | compact msgpack -> bytes | Named-key msgpack map |
| `encode(cls_name, bytes)` | named-key msgpack bytes -> bytes | Compact msgpack |
| `decode_json(cls_name, bytes)` | compact msgpack -> dict | Convenience: returns Python dict |
| `encode_json(cls_name, dict)` | Python dict -> bytes | Convenience: accepts Python dict |
| `list_classes()` | -> list[str] | All `MessagePackObject` class names |
| `get_schema(cls_name)` | -> dict | Schema fields, key types, isStringKeyed |
| `avro_schema(cls_name, ...)` | -> dict | Avro record schema |
| `close()` | | Release native resources |

## Binding the native library

Pass `lib_path` or set `UNITY_MSGPACK_NATIVE_LIB` to the `.so`/`.dylib`/`.dll` path if
it is not found automatically.

## Development

This project folder **should be** under the C# project. Do not change the project layout. Keep what its file struture likes when you clone the repository.

```bash
cd ./pybind

# Install dev dependency
uv sync --extra dev

# Format
uv run ruff format .
uv run ruff check . --fix

# Test
uv run --extra dev pytest tests/ -v
```
