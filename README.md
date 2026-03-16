# UnityMsgpackSchemaExporter

A tool for extracting MessagePack schemas from Unity game assemblies (DummyDll) or any other .NET assembly, and converting between compact (keyless) msgpack and named-key msgpack or JSON.

[MessagePack-CSharp](https://github.com/MessagePack-CSharp/MessagePack-CSharp) makes you able to strip the original payload field name using `[Key(<int>)]` .This tool can read the assemblies, reconstruct the original msgpack schema by inspecting C# class defination, and lets you decode/encode the msgpack/json, resumes the key with field name define in C# class.

---

## CLI

```bash
unityMsgpackSchemaExporter <command> [options] --dll <path> 
```

The `--dll` option accepts either a directory or a single `.dll` file.

The path can also be set via environment variable `MSGPACK_ASSEMBLY_PATH`.

### Commands

#### `list [filter]`

List all `[MessagePackObject]` classes found in the loaded assemblies.

```bash
unityMsgpackSchemaExporter list --dll ./DummyDll
unityMsgpackSchemaExporter list MyNamespace.MyClass --dll ./DummyDll   # filter by name
```

#### `schema <className>`

Show the schema for a specific class.

```bash
unityMsgpackSchemaExporter schema MyNamespace.MyClass --dll ./DummyDll
unityMsgpackSchemaExporter schema MyNamespace.MyClass --json --dll ./DummyDll
```

#### `avro <className>`

Export the [Avro](https://github.com/apache/avro) schema for a class.

```bash
unityMsgpackSchemaExporter avro MyNamespace.MyClass --dll ./DummyDll
unityMsgpackSchemaExporter avro MyNamespace.MyClass --flat --dll ./DummyDll       # flat mode
unityMsgpackSchemaExporter avro MyNamespace.MyClass --nullable --dll ./DummyDll   # all-nullable
unityMsgpackSchemaExporter avro MyNamespace.MyClass --flat --nullable --dll ./DummyDll
```

Export all schemas at once:

```bash
unityMsgpackSchemaExporter avro --all --out ./schemas/ --dll ./DummyDll
unityMsgpackSchemaExporter avro --all --flat --out ./schemas/ --dll ./DummyDll
```

See [Avro Export Options](#avro-export-options) for details.

#### `decode <className> <data>`

Decode a compact msgpack binary into named-key JSON.

```bash
# Hex string
unityMsgpackSchemaExporter decode MyNamespace.MyClass 92012a --dll ./DummyDll

# Base64 string
unityMsgpackSchemaExporter decode MyNamespace.MyClass kgEq --dll ./DummyDll

# Read binary from file
unityMsgpackSchemaExporter decode MyNamespace.MyClass @/path/to/packet.bin --dll ./DummyDll
```

#### `encode <className> <json>`

Encode named-key JSON into a compact msgpack binary.

```bash
unityMsgpackSchemaExporter encode MyNamespace.MyClass '{"id":1,"description":"test"}' --dll ./DummyDll
unityMsgpackSchemaExporter encode MyNamespace.MyClass '{"id":1}' --hex --dll ./DummyDll
unityMsgpackSchemaExporter encode MyNamespace.MyClass @input.json --out output.bin --dll ./DummyDll
```

#### `to-named <className> <data>`

Convert compact msgpack binary into named-key msgpack (no JSON intermediate).

```bash
# Hex string
unityMsgpackSchemaExporter to-named MyNamespace.MyClass 92012a --hex --dll ./DummyDll

# Base64 string
unityMsgpackSchemaExporter to-named MyNamespace.MyClass kgEq --base64 --dll ./DummyDll

# Read from file, write to file
unityMsgpackSchemaExporter to-named MyNamespace.MyClass @packet.bin --out named.bin --dll ./DummyDll
```

#### `to-compact <className> <data>`

Convert named-key msgpack back into compact msgpack binary.

```bash
unityMsgpackSchemaExporter to-compact MyNamespace.MyClass 82a26964... --hex --dll ./DummyDll
unityMsgpackSchemaExporter to-compact MyNamespace.MyClass @named.bin --out compact.bin --dll ./DummyDll
```

#### `raw-decode <data>`

Decode msgpack binary without a schema (shows raw types and values).

```bash
unityMsgpackSchemaExporter raw-decode 92012a
```

#### `stats`

Show statistics about the loaded assemblies.

```bash
unityMsgpackSchemaExporter stats --dll ./DummyDll
```

#### `diff <dllPath1> <dllPath2>`

Compare schemas between two versions of the DummyDll.

```bash
unityMsgpackSchemaExporter diff ./DummyDll/ ../v2/DummyDll/
```

---

## Avro Export Options

### Nested (default)

Nested types are inlined as Avro `record` definitions inside the parent schema.
Useful for standalone schema files.

```bash
unityMsgpackSchemaExporter avro MyMsgPacks
```

```json
{
  "type": "record",
  "name": "MyMsgPacks",
  "fields": [
    {
      "name": "levels",
      "type": {
        "type": "array",
        "items": { "type": "record", "name": "MyMsgPacksNest", "fields": [...] }
      }
    }
  ]
}
```

### Flat (`--flat`)

Nested types are referenced by name only. The consumer is responsible for resolving named types (typically by loading all schemas together in a schema registry).

```bash
unityMsgpackSchemaExporter avro MyMsgPacks --flat
```

```json
{
  "type": "record",
  "name": "MyMsgPacks",
  "fields": [
    { "name": "levels", "type": { "type": "array", "items": "MyMsgPacksNest" } }
  ]
}
```

### All Nullable (`--nullable`)

Every field is wrapped as a `["null", <type>]` union.

```bash
unityMsgpackSchemaExporter avro MyNamespace.MyClass --nullable
```

```json
{
  "fields": [
    { "name": "id",      "type": ["null", "int"],    "default": null },
    { "name": "groupId", "type": ["null", "int"],    "default": null }
  ]
}
```

Both options can be combined as `--flat --nullable`.

### Custom Avro Fields

The exported Avro schemas include several custom fields to preserve
MessagePack-specific metadata that standard Avro cannot represent:

| Custom field | Scope | Description |
|---|---|---|
| `msgpack_key` | field | The original `[Key()]` value (int or string) used in the compact binary |
| `msgpack_unions` | record | `[Union()]` sub-type definitions `[{id, typeName}]` |
| `msgpack_key_type` | map | Original C# key type when the dictionary key is not `string` (e.g. `"int"`, `"long"`) |

### Non-string map keys and `msgpack_key_type`

Avro `map` requires string keys.  For `Dictionary<K, V>` where `K` is not
`string`, the exporter still emits an Avro `map`, but the non-string keys are
serialized as their JSON string representation (e.g. `"42"` for an `int` key,
`"\\uXXXX"` escape sequences for `byte[]` keys).  A custom `"msgpack_key_type"`
field records the original C# type so downstream tooling can deserialize keys
back to the correct type:

```json
{
  "type": "map",
  "values": "string",
  "msgpack_key_type": "int"
}
```

### Limitation

Theoretically, msgpack allows any supported type to serve as a key within a map. This inevitably leads to situations where certain languages (including JSON-based structures) cannot perfectly deserialize msgpack into their native types. While we add custom fields in the Avro schema to represent the full msgpack structure as accurately as possible, extremely rare edge cases may still fall outside our coverage. In practice, due to type constraints in C#, such unconventional key structures are highly unlikely to occur in real world production. Therefore, we provide only limited support for these edge scenarios.

---

## NativeAOT / FFI API

The `UnityMsgpackSchemaExporter.Native` project builds a shared library exposing a C ABI for use from any language that supports FFI (Python, Rust, Go, C, etc.).

[ABI reference](UnityMsgpackSchemaExporter.Native/README.md)

We also provide a simple out-of-box [python binding package](pybind/README.md) for it.

---

## C# Library

The core classes are in `UnityMsgpackSchemaExporter`. See more details [in this project](UnityMsgpackSchemaExporter/README.md).

---

## Building

### Build Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) or above

### CLI (single-file executable)

```bash
# Platform example:
# Linux: "linux-x64"; Windows: "win-x64"; macOS arm: "osx-arm64"
dotnet publish UnityMsgpackSchemaExporter.Cli -c Release -r $PLATFORM --self-contained true -p:PublishSingleFile=true -o dist/
```

### NativeAOT shared library

NativeAOT must be compiled on the target operating system for the native toolchain.

```bash
dotnet publish UnityMsgpackSchemaExporter.Native -f net8.0 -c Release -r $PLATFORM -o dist/native-$PLATFORM
```

---

## Multilang Examples

See [examples/README.md](examples/README.md) for C# (Lib), Rust (FFI), and Go/Python (Avro) usage examples.

---

## Testing

```bash
dotnet test UnityMsgpackSchemaExporter.Tests/UnityMsgpackSchemaExporter.Tests.csproj -v normal
```

---

## Special thanks

- Github Copilot
- Claude Opus 4.6

---

## License

MIT
