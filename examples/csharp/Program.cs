using MessagePack;
using System.Text.Json;
using System.Text.Json.Nodes;
using UnityMsgpackSchemaExporter;
using UnityMsgpackSchemaExporter.TestSchemas;

// ── Locate the TestSchemas assembly ────────────────────────────────────────────
var schemasDll = Path.Combine(
    AppContext.BaseDirectory,
    "UnityMsgpackSchemaExporter.TestSchemas.dll");

// ── Load schemas ───────────────────────────────────────────────────────────────
var extractor = new SchemaExtractor();
extractor.LoadAssembly(schemasDll);
extractor.Extract();

Console.WriteLine($"Loaded {extractor.Schemas.Count} schemas:");
foreach (var name in extractor.Schemas.Keys.OrderBy(x => x))
    Console.WriteLine($"  {name}");
Console.WriteLine();

var codec = new MsgPackCodec(extractor.Schemas);

// ═══════════════════════════════════════════════════════════════════════════════
// Example 1 — SimpleItem (string-keyed)
// ═══════════════════════════════════════════════════════════════════════════════
Console.WriteLine("=== Example 1: SimpleItem (string-keyed) ===");

var item = new SimpleItem { id = 1, name = "Sword", score = 9.5f, active = true, bigValue = 9999L };
byte[] compact = MessagePackSerializer.Serialize(item);
Console.WriteLine($"Compact msgpack ({compact.Length} bytes): {Convert.ToHexString(compact)}");

// Compact -> named-key msgpack
byte[] named = codec.Decode("SimpleItem", compact);
Console.WriteLine($"Named-key msgpack ({named.Length} bytes): {Convert.ToHexString(named)}");

// Compact -> JSON
JsonNode? json = codec.DecodeJson("SimpleItem", compact);
Console.WriteLine($"JSON: {json}");

// JSON -> compact msgpack (round-trip)
byte[] reEncoded = codec.EncodeJson("SimpleItem", json);
Console.WriteLine($"Re-encoded compact ({reEncoded.Length} bytes): {Convert.ToHexString(reEncoded)}");
Console.WriteLine($"Round-trip match: {compact.SequenceEqual(reEncoded)}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════════════════
// Example 2 — IntKeyedItem (integer-keyed)
// ═══════════════════════════════════════════════════════════════════════════════
Console.WriteLine("=== Example 2: IntKeyedItem (integer-keyed) ===");

var intItem = new IntKeyedItem { id = 42, label = "Shield", value = 3.14 };
byte[] intCompact = MessagePackSerializer.Serialize(intItem);
Console.WriteLine($"Compact msgpack: {Convert.ToHexString(intCompact)}");

JsonNode? intJson = codec.DecodeJson("IntKeyedItem", intCompact);
Console.WriteLine($"JSON (int-key -> string-key): {intJson}");

byte[] intNamed = codec.Decode("IntKeyedItem", intCompact);
Console.WriteLine($"Named msgpack: {Convert.ToHexString(intNamed)}");

byte[] intReEncoded = codec.Encode("IntKeyedItem", intNamed);
Console.WriteLine($"Re-encoded match: {intCompact.SequenceEqual(intReEncoded)}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════════════════
// Example 3 — Order with nested items (string-keyed, complex)
// ═══════════════════════════════════════════════════════════════════════════════
Console.WriteLine("=== Example 3: Order (nested types) ===");

var order = new Order
{
    orderId = 100,
    customer = "Alice",
    items = new List<OrderItem>
    {
        new() { productId = 1, quantity = 2, unitPrice = 9.99f },
        new() { productId = 2, quantity = 1, unitPrice = 49.99f }
    },
    tags = ["urgent", "gift"],
    metadata = new Dictionary<string, string> { ["note"] = "fragile" }
};

byte[] orderCompact = MessagePackSerializer.Serialize(order);
JsonNode? orderJson = codec.DecodeJson("Order", orderCompact);
Console.WriteLine($"Order JSON:\n{orderJson!.ToJsonString(new JsonSerializerOptions { WriteIndented = true })}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════════════════
// Example 4 — Avro schema export
// ═══════════════════════════════════════════════════════════════════════════════
Console.WriteLine("=== Example 4: Avro schema export ===");

var simpleSchema = extractor.FindSchema("SimpleItem")!;
var avro = AvroExporter.ExportSchema(simpleSchema, extractor.Schemas);
Console.WriteLine($"Avro for SimpleItem:\n{avro.ToJsonString(new JsonSerializerOptions { WriteIndented = true })}");
Console.WriteLine();

// With nullable option
var options = new AvroExportOptions { AllNullable = true };
var avroNullable = AvroExporter.ExportSchema(simpleSchema, extractor.Schemas, options);
Console.WriteLine($"Avro (all nullable):\n{avroNullable.ToJsonString(new JsonSerializerOptions { WriteIndented = true })}");
Console.WriteLine();

// ═══════════════════════════════════════════════════════════════════════════════
// Example 5 — Union type
// ═══════════════════════════════════════════════════════════════════════════════
Console.WriteLine("=== Example 5: Union type (UnionBase) ===");

var childA = new UnionChildA { label = "Alice", value = 42 };
byte[] unionCompact = MessagePackSerializer.Serialize<UnionBase>(childA);
Console.WriteLine($"Union compact (ChildA): {Convert.ToHexString(unionCompact)}");

JsonNode? unionJson = codec.DecodeJson("UnionBase", unionCompact);
Console.WriteLine($"Union JSON: {unionJson}");

var childB = new UnionChildB { message = "hero wins", enabled = true };
byte[] unionBCompact = MessagePackSerializer.Serialize<UnionBase>(childB);
JsonNode? unionBJson = codec.DecodeJson("UnionBase", unionBCompact);
Console.WriteLine($"Union JSON (ChildB): {unionBJson}");
Console.WriteLine();

Console.WriteLine("All examples completed successfully.");
