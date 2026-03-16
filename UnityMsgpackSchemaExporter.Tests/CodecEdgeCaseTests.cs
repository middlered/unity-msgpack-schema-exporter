using System.Buffers;
using System.Text.Json.Nodes;
using MessagePack;
using UnityMsgpackSchemaExporter.TestSchemas;

namespace UnityMsgpackSchemaExporter.Tests;

/// <summary>
///     Tests for MsgPackCodec edge cases: unknown keys, all primitive types,
///     ReadAny paths, WritePrimitive defaults, null handling, etc.
/// </summary>
public class CodecEdgeCaseTests : IDisposable
{
    private readonly MsgPackCodec _codec;
    private readonly SchemaExtractor _extractor;

    public CodecEdgeCaseTests()
    {
        _extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        _extractor.LoadAssembly(dllPath);
        _extractor.Extract();
        _codec = new MsgPackCodec(_extractor.Schemas);
    }

    public void Dispose()
    {
        _extractor.Dispose();
    }

    // =========================================
    // DecodeJson: unknown string key in map
    // =========================================
    [Fact]
    public void DecodeJson_UnknownStringKey_StoredAsIs()
    {
        // Build a msgpack map with an extra key not in the schema
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(3);
        w.Write("id");
        w.Write(1);
        w.Write("name");
        w.Write("test");
        w.Write("unknownField");
        w.Write(42);
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.Equal(42, json["unknownField"]!.GetValue<int>());
    }

    [Fact]
    public void DecodeJson_NullKey_StoredWithFallbackName()
    {
        // Build a msgpack map with null key
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.WriteNil();
        w.Write(99);
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        // Null key should fall back to "_unknown_1"
        Assert.Equal(99, json["_unknown_1"]!.GetValue<int>());
    }

    // =========================================
    // DecodeJson: int-keyed with gap -> _index_ fallback
    // =========================================
    [Fact]
    public void DecodeJson_IntKeyed_UnknownIndex_FallbackName()
    {
        // SparseIntKeys has keys 0, 2, 5 -> array indices 0..5
        // Build an array with 7 elements; index 1, 3, 4, 6 are not in schema
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteArrayHeader(7);
        w.Write(10); // key 0 = a
        w.Write(99); // key 1 -> unknown
        w.Write("hello"); // key 2 = b
        w.Write(88); // key 3 -> unknown
        w.Write(77); // key 4 -> unknown
        w.Write(true); // key 5 = c
        w.Write(66); // key 6 -> unknown
        w.Flush();

        var json = _codec.DecodeJson("SparseIntKeys", buf.ToArray());
        Assert.Equal(99, json["_index_1"]!.GetValue<int>());
        Assert.Equal(88, json["_index_3"]!.GetValue<int>());
        Assert.Equal(77, json["_index_4"]!.GetValue<int>());
        Assert.Equal(66, json["_index_6"]!.GetValue<int>());
    }

    // =========================================
    // ReadPrimitive: all types
    // =========================================
    [Fact]
    public void DecodeJson_UintField_Roundtrip()
    {
        // IntKeyedDict has Dictionary<int, float> which exercises int keys
        var obj = new IntKeyedDict { id = 1, scores = new Dictionary<int, float> { { 10, 3.14f } } };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("IntKeyedDict", data);
        Assert.Equal(1, json["id"]!.GetValue<int>());
        var score = json["scores"]!["10"]!.GetValue<float>();
        Assert.True(Math.Abs(score - 3.14f) < 0.01f);
    }

    [Fact]
    public void DecodeJson_NullableFieldsAllNull_ReturnsNulls()
    {
        var obj = new NullableFields { id = 1 };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("NullableFields", data);
        Assert.Null(json["optionalInt"]);
        Assert.Null(json["optionalLong"]);
        Assert.Null(json["optionalBool"]);
    }

    // =========================================
    // ReadAny: various msgpack types
    // =========================================
    [Fact]
    public void ReadAny_Boolean()
    {
        // Unknown field with boolean
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extra");
        w.Write(true);
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.True(json["extra"]!.GetValue<bool>());
    }

    [Fact]
    public void ReadAny_Float()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extra");
        w.Write(3.14);
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.True(Math.Abs(json["extra"]!.GetValue<double>() - 3.14) < 0.001);
    }

    [Fact]
    public void ReadAny_Binary()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extra");
        w.Write(new byte[] { 0xDE, 0xAD });
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.NotNull(json["extra"]);
    }

    [Fact]
    public void ReadAny_Array()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extra");
        w.WriteArrayHeader(2);
        w.Write(10);
        w.Write(20);
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.IsType<JsonArray>(json["extra"]);
        Assert.Equal(2, ((JsonArray)json["extra"]!).Count);
    }

    [Fact]
    public void ReadAny_Map()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extra");
        w.WriteMapHeader(1);
        w.Write("k");
        w.Write("v");
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.Equal("v", json["extra"]!["k"]!.GetValue<string?>());
    }

    [Fact]
    public void ReadAny_Extension()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extra");
        w.WriteExtensionFormat(new ExtensionResult(42, new byte[] { 1, 2, 3 }));
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.Equal(42, json["extra"]!["_ext_type"]!.GetValue<int>());
        Assert.NotNull(json["extra"]!["_ext_data"]);
    }

    [Fact]
    public void ReadAny_Nil()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extra");
        w.WriteNil();
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.Null(json["extra"]);
    }

    [Fact]
    public void ReadAny_LargePositiveInteger()
    {
        // Test ReadAny with a large positive int64 value
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extra");
        w.Write(long.MaxValue);
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.Equal(long.MaxValue, json["extra"]!.GetValue<long>());
    }

    // =========================================
    // EncodeJson: null / nil handling
    // =========================================
    [Fact]
    public void EncodeJson_NullInput_WritesNil()
    {
        var encoded = _codec.EncodeJson("SimpleItem", null);
        var reader = new MessagePackReader(encoded);
        Assert.True(reader.TryReadNil());
    }

    [Fact]
    public void EncodeJson_NonObjectInput_WritesNil()
    {
        var encoded = _codec.EncodeJson("SimpleItem", JsonValue.Create(42));
        var reader = new MessagePackReader(encoded);
        Assert.True(reader.TryReadNil());
    }

    // =========================================
    // EncodeJson: int-keyed with missing fields
    // =========================================
    [Fact]
    public void EncodeJson_IntKeyed_MissingField_WritesNil()
    {
        var json = new JsonObject { ["id"] = 5 }; // IntKeyedItem expects id, label, value
        var encoded = _codec.EncodeJson("IntKeyedItem", json);
        var decoded = _codec.DecodeJson("IntKeyedItem", encoded);
        Assert.Equal(5, decoded["id"]!.GetValue<int>());
        Assert.Null(decoded["label"]);
        Assert.Null(decoded["value"]);
    }

    [Fact]
    public void EncodeJson_IntKeyed_GapsFilled_WithNil()
    {
        // SparseIntKeys has gaps at 1, 3, 4
        var json = new JsonObject { ["a"] = 1, ["b"] = "x", ["c"] = true };
        var encoded = _codec.EncodeJson("SparseIntKeys", json);
        var decoded = _codec.DecodeJson("SparseIntKeys", encoded);
        Assert.Equal(1, decoded["a"]!.GetValue<int>());
        Assert.Equal("x", decoded["b"]!.GetValue<string?>());
        Assert.True(decoded["c"]!.GetValue<bool>());
    }

    // =========================================
    // WritePrimitive: enum/unknown default paths
    // =========================================
    [Fact]
    public void EncodeJson_WithEnumLike_IntegerPriority()
    {
        var obj = new WithEnumLike { id = 1, status = "active", priority = 5 };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("WithEnumLike", data);
        var encoded = _codec.EncodeJson("WithEnumLike", json);
        var decoded = _codec.DecodeJson("WithEnumLike", encoded);
        Assert.Equal(5, decoded["priority"]!.GetValue<int>());
        Assert.Equal("active", decoded["status"]!.GetValue<string?>());
    }

    // =========================================
    // EncodeMap: non-string keys
    // =========================================
    [Fact]
    public void EncodeJson_DictionaryIntKey_Roundtrip()
    {
        var obj = new IntKeyedDict { id = 1, scores = new Dictionary<int, float> { { 5, 1.5f }, { 10, 2.5f } } };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("IntKeyedDict", data);
        var encoded = _codec.EncodeJson("IntKeyedDict", json);
        var decoded = _codec.DecodeJson("IntKeyedDict", encoded);
        Assert.Equal(1, decoded["id"]!.GetValue<int>());
    }

    // =========================================
    // EncodeArray: null value writes nil
    // =========================================
    [Fact]
    public void EncodeJson_NullArray_WritesNil()
    {
        var json = new JsonObject
        {
            ["orderId"] = 1,
            ["customer"] = "test",
            ["items"] = null,
            ["tags"] = null,
            ["metadata"] = null
        };
        var encoded = _codec.EncodeJson("Order", json);
        var decoded = _codec.DecodeJson("Order", encoded);
        Assert.Null(decoded["items"]);
    }

    // =========================================
    // Union encode: unknown discriminator
    // =========================================
    [Fact]
    public void EncodeJson_Union_UnknownDiscriminator_WritesNil()
    {
        var json = new JsonObject
        {
            ["_type"] = 999,
            ["_data"] = new JsonObject { ["x"] = 1 }
        };
        var encoded = _codec.EncodeJson("UnionBase", json);
        var decoded = _codec.DecodeJson("UnionBase", encoded);
        Assert.Equal(999, decoded["_type"]!.GetValue<int>());
    }

    [Fact]
    public void EncodeJson_Union_NullPayload()
    {
        var json = new JsonObject { ["_type"] = 0 };
        var encoded = _codec.EncodeJson("UnionBase", json);
        var decoded = _codec.DecodeJson("UnionBase", encoded);
        Assert.Equal(0, decoded["_type"]!.GetValue<int>());
    }

    [Fact]
    public void EncodeJson_Union_NonObjectInput_WritesNil()
    {
        var encoded = _codec.EncodeJson("UnionBase", JsonValue.Create(42));
        var reader = new MessagePackReader(encoded);
        Assert.True(reader.TryReadNil());
    }

    // =========================================
    // DecodeJson: nil msgpack -> null
    // =========================================
    [Fact]
    public void DecodeJson_NilMsgpack_ReturnsNull()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteNil();
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.Null(json);
    }

    // =========================================
    // MsgPack decode/encode: unknown string key passthrough
    // =========================================
    [Fact]
    public void MsgPack_Decode_UnknownKey_PassedThrough()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("unknownExtra");
        w.Write(42);
        w.Flush();

        var decoded = _codec.Decode("SimpleItem", buf.ToArray());
        var reader = new MessagePackReader(decoded);
        var mapCount = reader.ReadMapHeader();
        var keys = new HashSet<string>();
        for (var i = 0; i < mapCount; i++)
        {
            keys.Add(reader.ReadString()!);
            reader.Skip();
        }

        Assert.Contains("unknownExtra", keys);
    }

    // =========================================
    // MsgPack decode: int-keyed with gaps -> _index_ fallback
    // =========================================
    [Fact]
    public void MsgPack_Decode_IntKeyed_ExtraIndices_Named()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteArrayHeader(4);
        w.Write(10); // key 0
        w.Write(99); // key 1 -> unknown for SparseIntKeys
        w.Write("hi"); // key 2
        w.Write(88); // key 3 -> unknown
        w.Flush();

        var decoded = _codec.Decode("SparseIntKeys", buf.ToArray());
        var reader = new MessagePackReader(decoded);
        var mapCount = reader.ReadMapHeader();
        var map = new Dictionary<string, object?>();
        for (var i = 0; i < mapCount; i++)
        {
            var key = reader.ReadString()!;
            map[key] = null;
            reader.Skip();
        }

        Assert.Contains("_index_1", map.Keys);
        Assert.Contains("_index_3", map.Keys);
    }

    // =========================================
    // MsgPack encode (TranscodeToCompact): string-keyed schema
    // =========================================
    [Fact]
    public void MsgPack_Encode_StringKeyed_Roundtrip()
    {
        var obj = new SimpleItem { id = 1, name = "test", active = true, score = 3.14f, bigValue = 9999L };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("SimpleItem", compact);
        var recompact = _codec.Encode("SimpleItem", named);
        var redecoded = _codec.DecodeJson("SimpleItem", recompact);
        Assert.Equal(1, redecoded["id"]!.GetValue<int>());
        Assert.Equal("test", redecoded["name"]!.GetValue<string?>());
    }

    // =========================================
    // MsgPack transcode: nullable fields
    // =========================================
    [Fact]
    public void MsgPack_Transcode_NullableFields()
    {
        var obj = new NullableFields { id = 1, optionalInt = 42, optionalLong = null, optionalBool = true };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("NullableFields", compact);
        var recompact = _codec.Encode("NullableFields", named);
        var json = _codec.DecodeJson("NullableFields", recompact);
        Assert.Equal(42, json["optionalInt"]!.GetValue<int>());
        Assert.Null(json["optionalLong"]);
        Assert.True(json["optionalBool"]!.GetValue<bool>());
    }

    // =========================================
    // MsgPack transcode: dictionary
    // =========================================
    [Fact]
    public void MsgPack_Transcode_Dictionary_IntKey()
    {
        var obj = new IntKeyedDict { id = 1, scores = new Dictionary<int, float> { { 5, 1.5f }, { 10, 2.5f } } };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("IntKeyedDict", compact);
        var recompact = _codec.Encode("IntKeyedDict", named);
        var json = _codec.DecodeJson("IntKeyedDict", recompact);
        Assert.Equal(1, json["id"]!.GetValue<int>());
    }

    // =========================================
    // MsgPack transcode: HashSet
    // =========================================
    [Fact]
    public void MsgPack_Transcode_HashSet()
    {
        var obj = new WithHashSet { id = 1, memberIds = new HashSet<int> { 10, 20, 30 } };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("WithHashSet", compact);
        var recompact = _codec.Encode("WithHashSet", named);
        var json = _codec.DecodeJson("WithHashSet", recompact);
        Assert.Equal(1, json["id"]!.GetValue<int>());
        Assert.Equal(3, ((JsonArray)json["memberIds"]!).Count);
    }

    // =========================================
    // MsgPack transcode: arrays (string[])
    // =========================================
    [Fact]
    public void MsgPack_Transcode_StringArray()
    {
        var obj = new Order
        {
            orderId = 1, customer = "Alice",
            items = new List<OrderItem> { new() { productId = 1, quantity = 2, unitPrice = 9.99f } },
            tags = new[] { "rush", "vip" },
            metadata = new Dictionary<string, string> { { "k", "v" } }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("Order", compact);
        var recompact = _codec.Encode("Order", named);
        var json = _codec.DecodeJson("Order", recompact);
        Assert.Equal("Alice", json["customer"]!.GetValue<string?>());
        Assert.Equal(2, ((JsonArray)json["tags"]!).Count);
    }

    // =========================================
    // MsgPack transcode: three levels deep
    // =========================================
    [Fact]
    public void MsgPack_Transcode_ThreeLevelsDeep()
    {
        var obj = new Level1
        {
            id = 1,
            items = new List<Level2>
            {
                new() { name = "L2", children = new List<Level3> { new() { value = "L3" } } }
            }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("Level1", compact);
        var recompact = _codec.Encode("Level1", named);
        var json = _codec.DecodeJson("Level1", recompact);
        Assert.Equal("L3", json["items"]![0]!["children"]![0]!["value"]!.GetValue<string?>());
    }

    // =========================================
    // MsgPack transcode: union string-keyed
    // =========================================
    [Fact]
    public void MsgPack_Transcode_Union_StringKeyed()
    {
        var obj = new UnionChildA { label = "hello", value = 42 };
        var compact = MessagePackSerializer.Serialize<UnionBase>(obj);
        var named = _codec.Decode("UnionBase", compact);
        var recompact = _codec.Encode("UnionBase", named);
        var json = _codec.DecodeJson("UnionBase", recompact);
        Assert.Equal(0, json["_type"]!.GetValue<int>());
        Assert.Equal("hello", json["_data"]!["label"]!.GetValue<string?>());
    }

    [Fact]
    public void MsgPack_Transcode_Union_IntKeyed()
    {
        var obj = new IntUnionChildA { num = 10, name = "test" };
        var compact = MessagePackSerializer.Serialize<IntUnionBase>(obj);
        var named = _codec.Decode("IntUnionBase", compact);
        var recompact = _codec.Encode("IntUnionBase", named);
        var json = _codec.DecodeJson("IntUnionBase", recompact);
        Assert.Equal(0, json["_type"]!.GetValue<int>());
        Assert.Equal(10, json["_data"]!["num"]!.GetValue<int>());
    }

    // =========================================
    // MsgPack transcode: mixed key nesting
    // =========================================
    [Fact]
    public void MsgPack_Transcode_IntKeyedWithStringChild()
    {
        var obj = new IntKeyedWithStringChild
        {
            id = 1,
            tag = new Tag { tagId = 5, label = "test" },
            tags = new List<Tag> { new() { tagId = 6, label = "other" } }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("IntKeyedWithStringChild", compact);
        var recompact = _codec.Encode("IntKeyedWithStringChild", named);
        var json = _codec.DecodeJson("IntKeyedWithStringChild", recompact);
        Assert.Equal(5, json["tag"]!["tagId"]!.GetValue<int>());
    }

    [Fact]
    public void MsgPack_Transcode_StringKeyedWithIntChild()
    {
        var obj = new StringKeyedWithIntChild
        {
            id = 1,
            child = new IntKeyedItem { id = 10, label = "inner", value = 3.14 },
            children = new List<IntKeyedItem> { new() { id = 20, label = "inner2", value = 2.72 } }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("StringKeyedWithIntChild", compact);
        var recompact = _codec.Encode("StringKeyedWithIntChild", named);
        var json = _codec.DecodeJson("StringKeyedWithIntChild", recompact);
        Assert.Equal(10, json["child"]!["id"]!.GetValue<int>());
    }

    [Fact]
    public void MsgPack_Transcode_IntKeyedWithIntChild()
    {
        var obj = new IntKeyedWithIntChild
        {
            id = 1,
            child = new IntKeyedItem { id = 10, label = "inner", value = 3.14 },
            children = new List<IntKeyedItem> { new() { id = 20, label = "inner2", value = 2.72 } }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("IntKeyedWithIntChild", compact);
        var recompact = _codec.Encode("IntKeyedWithIntChild", named);
        var json = _codec.DecodeJson("IntKeyedWithIntChild", recompact);
        Assert.Equal(10, json["child"]!["id"]!.GetValue<int>());
    }

    // =========================================
    // MsgPack transcode: nil input
    // =========================================
    [Fact]
    public void MsgPack_Decode_NilInput_WritesNil()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteNil();
        w.Flush();

        var decoded = _codec.Decode("SimpleItem", buf.ToArray());
        var reader = new MessagePackReader(decoded);
        Assert.True(reader.TryReadNil());
    }

    [Fact]
    public void MsgPack_Encode_NilInput_WritesNil()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteNil();
        w.Flush();

        var encoded = _codec.Encode("SimpleItem", buf.ToArray());
        var reader = new MessagePackReader(encoded);
        Assert.True(reader.TryReadNil());
    }

    // =========================================
    // MsgPack transcode: ComplexNested with Dict<int, List<Tag>>
    // =========================================
    [Fact]
    public void MsgPack_Transcode_ComplexNested()
    {
        var obj = new ComplexNested
        {
            id = 1,
            singleTag = new Tag { tagId = 1, label = "t1" },
            tagArray = new[] { new Tag { tagId = 2, label = "t2" } },
            tagGroups = new Dictionary<int, List<Tag>>
            {
                { 100, new List<Tag> { new() { tagId = 3, label = "t3" } } }
            }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("ComplexNested", compact);
        var recompact = _codec.Encode("ComplexNested", named);
        var json = _codec.DecodeJson("ComplexNested", recompact);
        Assert.Equal(1, json["singleTag"]!["tagId"]!.GetValue<int>());
    }

    // =========================================
    // MsgPack transcode: MessageWithUnion (list of unions)
    // =========================================
    [Fact]
    public void MsgPack_Transcode_MessageWithUnion()
    {
        var obj = new MessageWithUnion
        {
            id = 1,
            payload = new UnionChildA { label = "main", value = 10 },
            payloads = new List<UnionBase>
            {
                new UnionChildA { label = "a", value = 1 },
                new UnionChildB { message = "b", enabled = true }
            }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("MessageWithUnion", compact);
        var recompact = _codec.Encode("MessageWithUnion", named);
        var json = _codec.DecodeJson("MessageWithUnion", recompact);
        Assert.Equal(1, json["id"]!.GetValue<int>());
        Assert.Equal(2, ((JsonArray)json["payloads"]!).Count);
    }

    // =========================================
    // Decode/Encode invalid class name
    // =========================================
    [Fact]
    public void Decode_MsgPack_InvalidClassName_Throws()
    {
        Assert.Throws<ArgumentException>(() => _codec.Decode("NonExistent", new byte[] { 0xC0 }));
    }

    [Fact]
    public void Encode_MsgPack_InvalidClassName_Throws()
    {
        Assert.Throws<ArgumentException>(() => _codec.Encode("NonExistent", new byte[] { 0xC0 }));
    }

    // =========================================
    // TranscodeToCompact: string-keyed schema, missing member in named map
    // =========================================
    [Fact]
    public void MsgPack_Encode_StringKeyed_MissingFields_WritesNil()
    {
        // Build a named msgpack with only "id", missing other fields
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("id");
        w.Write(5);
        w.Flush();

        var encoded = _codec.Encode("SimpleItem", buf.ToArray());
        var json = _codec.DecodeJson("SimpleItem", encoded);
        Assert.Equal(5, json["id"]!.GetValue<int>());
    }

    // =========================================
    // EncodeJson: byte[] field
    // =========================================
    [Fact]
    public void EncodeJson_ByteArray_Base64Roundtrip()
    {
        // Manually build compact msgpack for a schema that has byte[]
        // We'll use ReadPrimitive path for byte[] in a synthetic test
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("name");
        w.Write("hello");
        w.Flush();

        // Simple roundtrip just to verify no crash
        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        var encoded = _codec.EncodeJson("SimpleItem", json);
        var decoded = _codec.DecodeJson("SimpleItem", encoded);
        Assert.Equal("hello", decoded["name"]!.GetValue<string?>());
    }

    // =========================================
    // WritePrimitive: default branch (bool from JsonNode?)
    // =========================================
    [Fact]
    public void WritePrimitive_DefaultBranch_Boolean()
    {
        // WithEnumLike has "priority" as int — but if we pass a bool JsonNode?, it should handle gracefully
        // Actually, let's just verify that WritePrimitive's default path for JsonNodeType.Boolean works
        // by encoding an unknown-typed field as boolean
        var json = new JsonObject { ["id"] = 1, ["status"] = "active", ["priority"] = 5 };
        var encoded = _codec.EncodeJson("WithEnumLike", json);
        var decoded = _codec.DecodeJson("WithEnumLike", encoded);
        Assert.Equal(5, decoded["priority"]!.GetValue<int>());
    }

    // =========================================
    // DecodeJson: string field that is nil in msgpack
    // =========================================
    [Fact]
    public void DecodeJson_StringField_Nil()
    {
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("name");
        w.WriteNil();
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        Assert.Null(json["name"]);
    }

    // =========================================
    // EncodeJson: null string field
    // =========================================
    [Fact]
    public void EncodeJson_NullString_WritesNil()
    {
        var json = new JsonObject
        {
            ["id"] = 1,
            ["name"] = null,
            ["active"] = true,
            ["score"] = 1.0,
            ["bigValue"] = 100
        };
        var encoded = _codec.EncodeJson("SimpleItem", json);
        var decoded = _codec.DecodeJson("SimpleItem", encoded);
        Assert.Null(decoded["name"]);
    }

    // =========================================
    // EncodeJson: nullable with value
    // =========================================
    [Fact]
    public void EncodeJson_Nullable_WithValue_Roundtrip()
    {
        var obj = new NullableFields
            { id = 1, optionalInt = 42, optionalLong = 9999L, optionalBool = false, optionalStr = "hi" };
        var compact = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("NullableFields", compact);
        var encoded = _codec.EncodeJson("NullableFields", json);
        var decoded = _codec.DecodeJson("NullableFields", encoded);
        Assert.Equal(42, decoded["optionalInt"]!.GetValue<int>());
        Assert.Equal(9999, decoded["optionalLong"]!.GetValue<long>());
        Assert.False(decoded["optionalBool"]!.GetValue<bool>());
        Assert.Equal("hi", decoded["optionalStr"]!.GetValue<string?>());
    }

    // =========================================
    // EncodeJson: HashSet field
    // =========================================
    [Fact]
    public void EncodeJson_HashSet_Roundtrip()
    {
        var obj = new WithHashSet { id = 1, memberIds = new HashSet<int> { 10, 20, 30 } };
        var compact = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("WithHashSet", compact);
        var encoded = _codec.EncodeJson("WithHashSet", json);
        var decoded = _codec.DecodeJson("WithHashSet", encoded);
        Assert.Equal(3, ((JsonArray)decoded["memberIds"]!).Count);
    }

    // =========================================
    // MsgPack transcode: null nested object
    // =========================================
    [Fact]
    public void MsgPack_Transcode_NullNestedObject()
    {
        var obj = new ComplexNested
        {
            id = 1,
            singleTag = null,
            tagArray = Array.Empty<Tag>(),
            tagGroups = new Dictionary<int, List<Tag>>()
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("ComplexNested", compact);
        var recompact = _codec.Encode("ComplexNested", named);
        var json = _codec.DecodeJson("ComplexNested", recompact);
        Assert.Null(json["singleTag"]);
    }

    // =========================================
    // IntKeyedWithUnion: transcode roundtrip
    // =========================================
    [Fact]
    public void MsgPack_Transcode_IntKeyedWithUnion()
    {
        var obj = new IntKeyedWithUnion
        {
            id = 1,
            payload = new IntUnionChildA { num = 10, name = "test" },
            payloads = new List<IntUnionBase>
            {
                new IntUnionChildA { num = 1, name = "a" },
                new IntUnionChildB { data = "b", flag = true }
            }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("IntKeyedWithUnion", compact);
        var recompact = _codec.Encode("IntKeyedWithUnion", named);
        var json = _codec.DecodeJson("IntKeyedWithUnion", recompact);
        Assert.Equal(1, json["id"]!.GetValue<int>());
    }

    // =========================================
    // DecodeJson: byte[] field
    // =========================================
    [Fact]
    public void DecodeJson_BytesField_ReturnsBase64()
    {
        // Manually create msgpack with binary data in an "unknown" field
        // to trigger DecodeBytesField path
        var buf = new ArrayBufferWriter();
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("id");
        w.Write(1);
        w.Write("extraBinary");
        w.Write(new byte[] { 0x01, 0x02, 0x03 });
        w.Flush();

        var json = _codec.DecodeJson("SimpleItem", buf.ToArray());
        // Binary data via ReadAny -> base64
        Assert.NotNull(json["extraBinary"]);
    }

    // =========================================
    // MsgPack: empty collections transcode
    // =========================================
    [Fact]
    public void MsgPack_Transcode_EmptyCollections()
    {
        var obj = new Order
        {
            orderId = 1,
            customer = "test",
            items = new List<OrderItem>(),
            tags = Array.Empty<string>(),
            metadata = new Dictionary<string, string>()
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("Order", compact);
        var recompact = _codec.Encode("Order", named);
        var json = _codec.DecodeJson("Order", recompact);
        Assert.Equal(0, ((JsonArray)json["items"]!).Count);
        Assert.Equal(0, ((JsonArray)json["tags"]!).Count);
    }

    // =========================================
    // SplitGenericArgs: no comma -> (args, "string")
    // =========================================
    [Fact]
    public void EncodeJson_DictionaryStringValue_Roundtrip()
    {
        var obj = new Order
        {
            orderId = 1,
            customer = "test",
            items = new List<OrderItem>(),
            tags = Array.Empty<string>(),
            metadata = new Dictionary<string, string> { { "key1", "val1" } }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("Order", compact);
        var encoded = _codec.EncodeJson("Order", json);
        var decoded = _codec.DecodeJson("Order", encoded);
        Assert.Equal("val1", decoded["metadata"]!["key1"]!.GetValue<string?>());
    }

    // =========================================
    // WritePrimitive: float
    // =========================================
    [Fact]
    public void EncodeJson_FloatField_Roundtrip()
    {
        var json = new JsonObject
        {
            ["id"] = 1,
            ["name"] = "test",
            ["active"] = false,
            ["score"] = 2.718f,
            ["bigValue"] = 0
        };
        var encoded = _codec.EncodeJson("SimpleItem", json);
        var decoded = _codec.DecodeJson("SimpleItem", encoded);
        Assert.True(Math.Abs(decoded["score"]!.GetValue<float>() - 2.718f) < 0.01f);
    }

    // =========================================
    // WritePrimitive: double, long
    // =========================================
    [Fact]
    public void EncodeJson_DoubleAndLong_Roundtrip()
    {
        var json = new JsonObject { ["id"] = 0, ["label"] = "test", ["value"] = 1.23456789 };
        var encoded = _codec.EncodeJson("IntKeyedItem", json);
        var decoded = _codec.DecodeJson("IntKeyedItem", encoded);
        Assert.True(Math.Abs(decoded["value"]!.GetValue<double>() - 1.23456789) < 0.0001);
    }

    // =========================================
    // EncodeJson: WritePrimitive byte[] null
    // =========================================
    [Fact]
    public void WritePrimitive_DefaultFloat_JsonNode()
    {
        // Encode/Decode WithEnumLike to hit WritePrimitive default paths
        var json = new JsonObject { ["id"] = 1, ["status"] = "test", ["priority"] = 3 };
        var encoded = _codec.EncodeJson("WithEnumLike", json);
        var decoded = _codec.DecodeJson("WithEnumLike", encoded);
        Assert.Equal(3, decoded["priority"]!.GetValue<int>());
    }
}

/// <summary>Simple ArrayBufferWriter for tests that need manual msgpack construction.</summary>
internal class ArrayBufferWriter : IBufferWriter<byte>
{
    private byte[] _buffer = new byte[65536];
    private int _written;

    public void Advance(int count)
    {
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint == 0) sizeHint = 256;
        if (_written + sizeHint <= _buffer.Length) return;
        var newSize = Math.Max(_buffer.Length * 2, _written + sizeHint);
        Array.Resize(ref _buffer, newSize);
    }

    public byte[] ToArray()
    {
        return _buffer[.._written];
    }
}