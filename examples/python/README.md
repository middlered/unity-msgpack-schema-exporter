# unity-msgpack-avro (Python)

Python library to decode/encode **compact Unity MessagePack** data using the Avro schemas exported by [Unity MsgPack Schema Tools](../../README.md).

## Features

- Parse Avro schema JSON files exported by the C# tool
- Decode compact (keyless) msgpack bytes into Python dicts/lists
- Encode Python dicts/lists back to compact msgpack
- Handles all project-specific extensions:
  - `msgpack_key` — per-field int or string key
  - `msgpack_key_type` — maps with non-string native keys (int, long, bytes, …)
  - `msgpack_unions` — polymorphic dispatch via `[discriminator, payload]`

## Requirements

- Python ≥ 3.11
- `msgpack` ≥ 1.1.2
- `msgspec` ≥ 0.20.0

## Installation

```bash
uv add unity-msgpack-avro
# or
pip install unity-msgpack-avro
```

## Usage

### 1 – Export Avro schema from C\#

```bash
dotnet unityMsgpackSchemaExporterCli.dll avro MyClass --dll MyAssembly.dll > schema.json
```

### 2 – Decode msgpack in Python

```python
from unity_msgpack_avro import load_schema, decode, encode

schema = load_schema("schema.json")
result = decode(schema, compact_bytes)
compact = encode(schema, result)
```

### 3 – Multi-schema files

```python
from unity_msgpack_avro import load_bytes

registry, root = load_bytes(Path("all_schemas.json").read_bytes())
schema = registry["MyNamespace.MyClass"]
result = decode(schema, compact_bytes)
```

### CLI

```bash
python cli.py decode --schema schema.json --class MyClass --hex a3010203
python cli.py decode --schema schema.json --file data.bin
```

## Decoded value structure

| Avro / msgpack type | Python type |
|---------------------|-------------|
| record (string-keyed) | `dict[str, Any]` keyed by field **name** |
| record (int-keyed) | `dict[str, Any]` keyed by field **name** |
| array | `list[Any]` |
| map (string keys) | `dict[str, Any]` |
| map (`msgpack_key_type`) | `dict[native_key, Any]` |
| union dispatch (`msgpack_unions`) | `{"__type": int, "value": Any}` |
| nullable union `["null", T]` | `None` or `T` |
| primitives | `bool`, `int`, `float`, `str`, `bytes` |

## Running tests

```bash
uv run pytest
```
