# C# Library API

## SchemaExtractor

Loads assemblies and extracts MessagePack schemas.

```csharp
var extractor = new SchemaExtractor();

// Load all assemblies from a directory
extractor.LoadDirectory("/path/to/DummyDll");

// Or load a single assembly
extractor.LoadAssembly("/path/to/Assembly-CSharp.dll");

// Get all schemas
IReadOnlyList<MsgPackClassSchema> schemas = extractor.Schemas;

// Find by name (simple or fully-qualified)
MsgPackClassSchema? schema = extractor.FindSchema("MyNamespace.MyClass");
```

## MsgPackClassSchema

Represents one `[MessagePackObject]` class.

```csharp
schema.FullName       // "Program.MyNamespace.MyClass"
schema.SimpleTypeName // "MyNamespace.MyClass"
schema.KeyType        // MsgPackKeyType.Int or MsgPackKeyType.String
schema.Fields         // IReadOnlyList<MsgPackFieldSchema>
schema.Unions         // IReadOnlyList<MsgPackUnionEntry>
```

## MsgPackCodec

Converts between compact (keyless) msgpack and named-key msgpack or JSON.

```csharp
var codec = new MsgPackCodec(extractor.Schemas);

// ── MsgPack <-> MsgPack (no JSON intermediate, preserves types) ──
byte[] named   = codec.Decode("MyClass", compactBytes);  // compact -> named-key msgpack
byte[] compact = codec.Encode("MyClass", namedBytes);    // named-key -> compact msgpack

// ── JSON convenience path ──
JsonNode json  = codec.DecodeJson("MyClass", compactBytes); // compact -> JsonNode
byte[] binary  = codec.EncodeJson("MyClass", jsonToken);    // JsonNode -> compact msgpack
```

| Method                      | Input -> Output                   | Description                              |
|-----------------------------|-----------------------------------|------------------------------------------|
| `Decode(cls, byte[])`       | compact msgpack -> byte[]         | Named-key msgpack map (no JSON overhead) |
| `Encode(cls, byte[])`       | named-key msgpack bytes -> byte[] | Compact msgpack (no JSON overhead)       |
| `DecodeJson(cls, byte[])`   | compact msgpack -> JsonNode       | Named-key JSON (JSON interop)            |
| `EncodeJson(cls, JsonNode)` | JsonNode -> byte[]                | Compact msgpack (JSON interop)           |

`Decode`/`Encode` are the default path — msgpack bytes in, msgpack bytes out.
`DecodeJson`/`EncodeJson` are convenience methods for JSON-based interop.

## AvroExporter

Exports Avro schemas.

```csharp
var exporter = new AvroExporter(extractor.Schemas);

// Default (nested, non-nullable)
string avroJson = exporter.ExportSchema("MyNamespace.MyClass");

// With options
var options = new AvroExportOptions { AllNullable = true, Flat = true };
string avroJson = exporter.ExportSchema("MyNamespace.MyClass", options);

// Export all schemas
List<string> allAvro = exporter.ExportAllSchemas(options);
```
