using System.Text.Json.Nodes;
using MessagePack;
using UnityMsgpackSchemaExporter.TestSchemas;

namespace UnityMsgpackSchemaExporter.Tests;

/// <summary>
///     Tests encode/decode roundtrip using real MessagePack serialization (from TestSchemas)
///     vs our schema-based codec.
/// </summary>
public class CodecRoundtripTests : IDisposable
{
    private readonly MsgPackCodec _codec;
    private readonly SchemaExtractor _extractor;

    public CodecRoundtripTests()
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

    [Fact]
    public void SimpleItem_Roundtrip()
    {
        var item = new SimpleItem { id = 42, name = "hello", active = true, score = 3.14f, bigValue = 9999999999L };
        var bytes = MessagePackSerializer.Serialize(item);

        // Decode with our codec
        var json = _codec.DecodeJson("SimpleItem", bytes);
        Assert.Equal(42, json["id"]!.GetValue<int>());
        Assert.Equal("hello", json["name"]!.GetValue<string?>());
        Assert.True(json["active"]!.GetValue<bool>());
        Assert.InRange(json["score"]!.GetValue<float>(), 3.13f, 3.15f);
        Assert.Equal(9999999999L, json["bigValue"]!.GetValue<long>());

        // Re-encode with our codec
        var reEncoded = _codec.EncodeJson("SimpleItem", json);

        // Decode again to verify roundtrip
        var json2 = _codec.DecodeJson("SimpleItem", reEncoded);
        Assert.Equal(42, json2["id"]!.GetValue<int>());
        Assert.Equal("hello", json2["name"]!.GetValue<string?>());
    }

    [Fact]
    public void IntKeyedItem_Roundtrip()
    {
        var item = new IntKeyedItem { id = 7, label = "test", value = 2.718 };
        var bytes = MessagePackSerializer.Serialize(item);

        var json = _codec.DecodeJson("IntKeyedItem", bytes);
        Assert.Equal(7, json["id"]!.GetValue<int>());
        Assert.Equal("test", json["label"]!.GetValue<string?>());
        Assert.InRange(json["value"]!.GetValue<double>(), 2.717, 2.719);

        var reEncoded = _codec.EncodeJson("IntKeyedItem", json);
        var json2 = _codec.DecodeJson("IntKeyedItem", reEncoded);
        Assert.Equal(7, json2["id"]!.GetValue<int>());
        Assert.Equal("test", json2["label"]!.GetValue<string?>());
    }

    [Fact]
    public void Order_NestedItems_Roundtrip()
    {
        var order = new Order
        {
            orderId = 100,
            customer = "Alice",
            items = new List<OrderItem>
            {
                new() { productId = 1, quantity = 2, unitPrice = 10.5f },
                new() { productId = 2, quantity = 1, unitPrice = 25.0f }
            },
            tags = new[] { "urgent", "vip" },
            metadata = new Dictionary<string, string>
            {
                ["region"] = "JP",
                ["source"] = "web"
            }
        };
        var bytes = MessagePackSerializer.Serialize(order);

        var json = _codec.DecodeJson("Order", bytes);
        Assert.Equal(100, json["orderId"]!.GetValue<int>());
        Assert.Equal("Alice", json["customer"]!.GetValue<string?>());

        // Nested items
        var items = json["items"] as JsonArray;
        Assert.NotNull(items);
        Assert.Equal(2, items!.Count);
        Assert.Equal(1, items[0]["productId"]!.GetValue<int>());
        Assert.Equal(2, items[0]["quantity"]!.GetValue<int>());
        Assert.InRange(items[0]["unitPrice"]!.GetValue<float>(), 10.4f, 10.6f);
        Assert.Equal(2, items[1]["productId"]!.GetValue<int>());

        // Tags
        var tags = json["tags"] as JsonArray;
        Assert.NotNull(tags);
        Assert.Equal(2, tags!.Count);
        Assert.Equal("urgent", tags[0]!.GetValue<string?>());

        // Metadata
        var meta = json["metadata"] as JsonObject;
        Assert.NotNull(meta);
        Assert.Equal("JP", meta!["region"]!.GetValue<string?>());

        // Roundtrip encode/decode
        var reEncoded = _codec.EncodeJson("Order", json);
        var json2 = _codec.DecodeJson("Order", reEncoded);
        Assert.Equal(100, json2["orderId"]!.GetValue<int>());
        var items2 = json2["items"] as JsonArray;
        Assert.Equal(2, items2!.Count);
        Assert.Equal(1, items2[0]["productId"]!.GetValue<int>());
    }

    [Fact]
    public void NullableFields_WithNulls()
    {
        var obj = new NullableFields
        {
            id = 1,
            optionalInt = null,
            optionalLong = 42L,
            optionalBool = null,
            optionalStr = null
        };
        var bytes = MessagePackSerializer.Serialize(obj);

        var json = _codec.DecodeJson("NullableFields", bytes);
        Assert.Equal(1, json["id"]!.GetValue<int>());
        Assert.Null(json["optionalInt"]);
        Assert.Equal(42L, json["optionalLong"]!.GetValue<long>());
        Assert.Null(json["optionalBool"]);

        // Roundtrip
        var reEncoded = _codec.EncodeJson("NullableFields", json);
        var json2 = _codec.DecodeJson("NullableFields", reEncoded);
        Assert.Null(json2["optionalInt"]);
        Assert.Equal(42L, json2["optionalLong"]!.GetValue<long>());
    }

    [Fact]
    public void NullableFields_WithValues()
    {
        var obj = new NullableFields
        {
            id = 2,
            optionalInt = 99,
            optionalLong = null,
            optionalBool = true,
            optionalStr = "hello"
        };
        var bytes = MessagePackSerializer.Serialize(obj);

        var json = _codec.DecodeJson("NullableFields", bytes);
        Assert.Equal(99, json["optionalInt"]!.GetValue<int>());
        Assert.True(json["optionalBool"]!.GetValue<bool>());
    }

    [Fact]
    public void ComplexNested_DictOfListOfTag()
    {
        var obj = new ComplexNested
        {
            id = 10,
            tagGroups = new Dictionary<int, List<Tag>>
            {
                [1] = new() { new Tag { tagId = 100, label = "A" }, new Tag { tagId = 101, label = "B" } },
                [2] = new() { new Tag { tagId = 200, label = "C" } }
            },
            singleTag = new Tag { tagId = 50, label = "single" },
            tagArray = new[] { new Tag { tagId = 60, label = "arr1" } }
        };
        var bytes = MessagePackSerializer.Serialize(obj);

        var json = _codec.DecodeJson("ComplexNested", bytes);
        Assert.Equal(10, json["id"]!.GetValue<int>());

        // tagGroups: dict<int, list<Tag>> -> JsonObject with string keys
        var groups = json["tagGroups"] as JsonObject;
        Assert.NotNull(groups);
        var g1 = groups!["1"] as JsonArray;
        Assert.NotNull(g1);
        Assert.Equal(2, g1!.Count);
        Assert.Equal(100, g1[0]["tagId"]!.GetValue<int>());
        Assert.Equal("A", g1[0]["label"]!.GetValue<string?>());

        // singleTag
        var single = json["singleTag"] as JsonObject;
        Assert.NotNull(single);
        Assert.Equal(50, single!["tagId"]!.GetValue<int>());
        Assert.Equal("single", single["label"]!.GetValue<string?>());

        // tagArray
        var arr = json["tagArray"] as JsonArray;
        Assert.NotNull(arr);
        Assert.Single(arr!);
        Assert.Equal(60, arr[0]["tagId"]!.GetValue<int>());

        // Roundtrip
        var reEncoded = _codec.EncodeJson("ComplexNested", json);
        var json2 = _codec.DecodeJson("ComplexNested", reEncoded);
        var groups2 = json2["tagGroups"] as JsonObject;
        var g1_2 = groups2!["1"] as JsonArray;
        Assert.Equal(100, g1_2![0]["tagId"]!.GetValue<int>());
    }

    [Fact]
    public void Level1_ThreeLevelDeep_Roundtrip()
    {
        var obj = new Level1
        {
            id = 1,
            items = new List<Level2>
            {
                new()
                {
                    name = "L2-A",
                    children = new List<Level3>
                    {
                        new() { value = "deep1" },
                        new() { value = "deep2" }
                    }
                },
                new()
                {
                    name = "L2-B",
                    children = new List<Level3>
                    {
                        new() { value = "deep3" }
                    }
                }
            }
        };
        var bytes = MessagePackSerializer.Serialize(obj);

        var json = _codec.DecodeJson("Level1", bytes);
        Assert.Equal(1, json["id"]!.GetValue<int>());

        var items = json["items"] as JsonArray;
        Assert.Equal(2, items!.Count);
        Assert.Equal("L2-A", items[0]["name"]!.GetValue<string?>());

        var children = items[0]["children"] as JsonArray;
        Assert.Equal(2, children!.Count);
        Assert.Equal("deep1", children[0]["value"]!.GetValue<string?>());

        // Roundtrip
        var reEncoded = _codec.EncodeJson("Level1", json);
        var json2 = _codec.DecodeJson("Level1", reEncoded);
        var items2 = json2["items"] as JsonArray;
        Assert.Equal("L2-A", items2![0]["name"]!.GetValue<string?>());
        var children2 = items2[0]["children"] as JsonArray;
        Assert.Equal("deep1", children2![0]["value"]!.GetValue<string?>());
    }

    [Fact]
    public void SparseIntKeys_GapsFilledWithNull()
    {
        var obj = new SparseIntKeys { a = 10, b = "hi", c = true };
        var bytes = MessagePackSerializer.Serialize(obj);

        var json = _codec.DecodeJson("SparseIntKeys", bytes);
        Assert.Equal(10, json["a"]!.GetValue<int>());
        Assert.Equal("hi", json["b"]!.GetValue<string?>());
        Assert.True(json["c"]!.GetValue<bool>());

        // Roundtrip
        var reEncoded = _codec.EncodeJson("SparseIntKeys", json);
        var json2 = _codec.DecodeJson("SparseIntKeys", reEncoded);
        Assert.Equal(10, json2["a"]!.GetValue<int>());
        Assert.True(json2["c"]!.GetValue<bool>());
    }

    [Fact]
    public void WithHashSet_Roundtrip()
    {
        var obj = new WithHashSet { id = 1, memberIds = new HashSet<int> { 10, 20, 30 } };
        var bytes = MessagePackSerializer.Serialize(obj);

        var json = _codec.DecodeJson("WithHashSet", bytes);
        Assert.Equal(1, json["id"]!.GetValue<int>());

        var members = json["memberIds"] as JsonArray;
        Assert.NotNull(members);
        Assert.Equal(3, members!.Count);

        // Roundtrip
        var reEncoded = _codec.EncodeJson("WithHashSet", json);
        var json2 = _codec.DecodeJson("WithHashSet", reEncoded);
        var members2 = json2["memberIds"] as JsonArray;
        Assert.Equal(3, members2!.Count);
    }

    [Fact]
    public void EmptyCollections_Roundtrip()
    {
        var order = new Order
        {
            orderId = 99,
            customer = "Bob",
            items = new List<OrderItem>(),
            tags = Array.Empty<string>(),
            metadata = new Dictionary<string, string>()
        };
        var bytes = MessagePackSerializer.Serialize(order);

        var json = _codec.DecodeJson("Order", bytes);
        Assert.Equal(99, json["orderId"]!.GetValue<int>());
        Assert.Empty((json["items"] as JsonArray)!);
        Assert.Empty((json["tags"] as JsonArray)!);

        var reEncoded = _codec.EncodeJson("Order", json);
        var json2 = _codec.DecodeJson("Order", reEncoded);
        Assert.Equal(99, json2["orderId"]!.GetValue<int>());
    }

    [Fact]
    public void NullNestedObject_Handled()
    {
        var obj = new ComplexNested
        {
            id = 5,
            singleTag = null,
            tagArray = Array.Empty<Tag>(),
            tagGroups = new Dictionary<int, List<Tag>>()
        };
        var bytes = MessagePackSerializer.Serialize(obj);

        var json = _codec.DecodeJson("ComplexNested", bytes);
        Assert.Equal(5, json["id"]!.GetValue<int>());
        Assert.Null(json["singleTag"]);
    }

    [Fact]
    public void Decode_InvalidClassName_Throws()
    {
        Assert.Throws<ArgumentException>(() => _codec.DecodeJson("NoSuchClass", new byte[] { 0xc0 }));
    }

    [Fact]
    public void Encode_InvalidClassName_Throws()
    {
        Assert.Throws<ArgumentException>(() => _codec.EncodeJson("NoSuchClass", new JsonObject()));
    }

    // ── MsgPack <-> MsgPack roundtrip tests ──────────────────────────────────

    [Fact]
    public void MsgPack_Decode_ProducesStringKeyedMap()
    {
        var item = new SimpleItem { id = 42, name = "hello", active = true, score = 3.14f, bigValue = 9999999999L };
        var compact = MessagePackSerializer.Serialize(item);

        var named = _codec.Decode("SimpleItem", compact);
        Assert.NotNull(named);
        Assert.True(named.Length > 0);

        // The named-key msgpack should be a map with string keys — verify with MessagePackReader
        var reader = new MessagePackReader(named);
        var mapCount = reader.ReadMapHeader();
        Assert.True(mapCount >= 5);

        // Collect keys
        var keys = new HashSet<string>();
        for (var i = 0; i < mapCount; i++)
        {
            keys.Add(reader.ReadString()!);
            reader.Skip(); // skip value
        }

        Assert.Contains("id", keys);
        Assert.Contains("name", keys);
        Assert.Contains("active", keys);
    }

    [Fact]
    public void MsgPack_Decode_PreservesIntDictionaryKeys()
    {
        // ComplexNested has Dictionary<int, List<Tag>> — keys should stay as integers in msgpack
        var obj = new ComplexNested
        {
            id = 1,
            tagGroups = new Dictionary<int, List<Tag>>
            {
                [42] = new() { new Tag { tagId = 1, label = "A" } },
                [99] = new() { new Tag { tagId = 2, label = "B" } }
            },
            singleTag = null,
            tagArray = Array.Empty<Tag>()
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("ComplexNested", compact);

        // Walk the named msgpack to find the tagGroups field and verify int keys
        var reader = new MessagePackReader(named);
        var topMap = reader.ReadMapHeader();
        for (var i = 0; i < topMap; i++)
        {
            var key = reader.ReadString()!;
            if (key == "tagGroups")
            {
                var dictCount = reader.ReadMapHeader();
                Assert.Equal(2, dictCount);
                // Keys should be integers, not strings
                var k1 = reader.ReadInt32();
                reader.Skip(); // skip value
                var k2 = reader.ReadInt32();
                reader.Skip();
                Assert.Contains(42, new[] { k1, k2 });
                Assert.Contains(99, new[] { k1, k2 });
                break;
            }

            reader.Skip();
        }
    }

    [Fact]
    public void MsgPack_Encode_Decode_Roundtrip()
    {
        var item = new SimpleItem { id = 7, name = "world", active = false, score = 2.5f, bigValue = 42L };
        var compact = MessagePackSerializer.Serialize(item);

        // compact -> named-key msgpack -> compact
        var named = _codec.Decode("SimpleItem", compact);
        var reCompact = _codec.Encode("SimpleItem", named);

        // Re-decode with JSON path to verify values preserved
        var result = _codec.DecodeJson("SimpleItem", reCompact);
        Assert.Equal(7, result["id"]!.GetValue<int>());
        Assert.Equal("world", result["name"]!.GetValue<string?>());
        Assert.False(result["active"]!.GetValue<bool>());
        Assert.InRange(result["score"]!.GetValue<float>(), 2.49f, 2.51f);
        Assert.Equal(42L, result["bigValue"]!.GetValue<long>());
    }

    [Fact]
    public void MsgPack_IntKeyed_Roundtrip()
    {
        var item = new IntKeyedItem { id = 100, label = "intkey", value = 9.99 };
        var compact = MessagePackSerializer.Serialize(item);

        var named = _codec.Decode("IntKeyedItem", compact);
        var reCompact = _codec.Encode("IntKeyedItem", named);

        var result = _codec.DecodeJson("IntKeyedItem", reCompact);
        Assert.Equal(100, result["id"]!.GetValue<int>());
        Assert.Equal("intkey", result["label"]!.GetValue<string?>());
        Assert.InRange(result["value"]!.GetValue<double>(), 9.98, 10.0);
    }

    [Fact]
    public void MsgPack_Order_NestedItems_Roundtrip()
    {
        var order = new Order
        {
            orderId = 55,
            customer = "Bob",
            items = new List<OrderItem> { new() { productId = 3, quantity = 10, unitPrice = 1.5f } },
            tags = new[] { "a", "b" },
            metadata = new Dictionary<string, string> { ["k"] = "v" }
        };
        var compact = MessagePackSerializer.Serialize(order);

        var named = _codec.Decode("Order", compact);
        var reCompact = _codec.Encode("Order", named);
        var result = _codec.DecodeJson("Order", reCompact);
        Assert.Equal(55, result["orderId"]!.GetValue<int>());
        Assert.Equal("Bob", result["customer"]!.GetValue<string?>());
        var items = result["items"] as JsonArray;
        Assert.Single(items!);
        Assert.Equal(3, items![0]["productId"]!.GetValue<int>());
    }

    [Fact]
    public void MsgPack_ComplexNested_DictIntKey_Roundtrip()
    {
        var obj = new ComplexNested
        {
            id = 10,
            tagGroups = new Dictionary<int, List<Tag>>
            {
                [1] = new() { new Tag { tagId = 100, label = "A" } },
                [2] = new() { new Tag { tagId = 200, label = "C" } }
            },
            singleTag = new Tag { tagId = 50, label = "single" },
            tagArray = new[] { new Tag { tagId = 60, label = "arr1" } }
        };
        var compact = MessagePackSerializer.Serialize(obj);

        var named = _codec.Decode("ComplexNested", compact);
        var reCompact = _codec.Encode("ComplexNested", named);
        var result = _codec.DecodeJson("ComplexNested", reCompact);

        Assert.Equal(10, result["id"]!.GetValue<int>());
        var groups = result["tagGroups"] as JsonObject;
        Assert.NotNull(groups);
        Assert.Equal(100, (groups!["1"] as JsonArray)![0]["tagId"]!.GetValue<int>());
    }

    [Fact]
    public void MsgPack_NullNestedObject_Handled()
    {
        var obj = new ComplexNested
        {
            id = 5,
            singleTag = null,
            tagArray = Array.Empty<Tag>(),
            tagGroups = new Dictionary<int, List<Tag>>()
        };
        var compact = MessagePackSerializer.Serialize(obj);

        var named = _codec.Decode("ComplexNested", compact);
        var reCompact = _codec.Encode("ComplexNested", named);
        var result = _codec.DecodeJson("ComplexNested", reCompact);
        Assert.Equal(5, result["id"]!.GetValue<int>());
    }

    [Fact]
    public void MsgPack_Level1_ThreeDeep_Roundtrip()
    {
        var obj = new Level1
        {
            id = 1,
            items = new List<Level2>
            {
                new() { name = "L2", children = new List<Level3> { new() { value = "deep" } } }
            }
        };
        var compact = MessagePackSerializer.Serialize(obj);

        var named = _codec.Decode("Level1", compact);
        var reCompact = _codec.Encode("Level1", named);
        var result = _codec.DecodeJson("Level1", reCompact);
        Assert.Equal(1, result["id"]!.GetValue<int>());
        var items = result["items"] as JsonArray;
        Assert.Equal("deep", (items![0]["children"] as JsonArray)![0]["value"]!.GetValue<string?>());
    }

    [Fact]
    public void MsgPack_SparseIntKeys_Roundtrip()
    {
        var obj = new SparseIntKeys { a = 10, b = "hi", c = true };
        var compact = MessagePackSerializer.Serialize(obj);

        var named = _codec.Decode("SparseIntKeys", compact);
        var reCompact = _codec.Encode("SparseIntKeys", named);
        var result = _codec.DecodeJson("SparseIntKeys", reCompact);
        Assert.Equal(10, result["a"]!.GetValue<int>());
        Assert.True(result["c"]!.GetValue<bool>());
    }

    [Fact]
    public void Decode_InvalidClassName_MsgPack_Throws()
    {
        Assert.Throws<ArgumentException>(() => _codec.Decode("NoSuchClass", new byte[] { 0x90 }));
    }

    [Fact]
    public void Encode_InvalidClassName_MsgPack_Throws()
    {
        Assert.Throws<ArgumentException>(() => _codec.Encode("NoSuchClass", new byte[] { 0x80 }));
    }

    // ── Mixed int/string key nesting tests ──────────────────────────────────

    [Fact]
    public void IntKeyedWithStringChild_Json_Roundtrip()
    {
        // int-keyed parent contains string-keyed Tag child
        var obj = new IntKeyedWithStringChild
        {
            id = 1,
            tag = new Tag { tagId = 10, label = "hello" },
            tags = new List<Tag> { new() { tagId = 20, label = "a" }, new() { tagId = 30, label = "b" } }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("IntKeyedWithStringChild", compact);

        Assert.Equal(1, json["id"]!.GetValue<int>());
        var tag = json["tag"] as JsonObject;
        Assert.Equal(10, tag!["tagId"]!.GetValue<int>());
        Assert.Equal("hello", tag["label"]!.GetValue<string?>());
        var tags = json["tags"] as JsonArray;
        Assert.Equal(2, tags!.Count);
        Assert.Equal(20, tags[0]["tagId"]!.GetValue<int>());

        var reEncoded = _codec.EncodeJson("IntKeyedWithStringChild", json);
        var json2 = _codec.DecodeJson("IntKeyedWithStringChild", reEncoded);
        Assert.Equal(1, json2["id"]!.GetValue<int>());
        Assert.Equal("hello", (json2["tag"] as JsonObject)!["label"]!.GetValue<string?>());
    }

    [Fact]
    public void IntKeyedWithStringChild_MsgPack_Roundtrip()
    {
        var obj = new IntKeyedWithStringChild
        {
            id = 5,
            tag = new Tag { tagId = 99, label = "test" },
            tags = new List<Tag> { new() { tagId = 1, label = "x" } }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("IntKeyedWithStringChild", compact);

        // Verify named map: top-level keys should be member names
        var reader = new MessagePackReader(named);
        var mapCount = reader.ReadMapHeader();
        var keys = new HashSet<string>();
        for (var i = 0; i < mapCount; i++)
        {
            keys.Add(reader.ReadString()!);
            reader.Skip();
        }

        Assert.Contains("id", keys);
        Assert.Contains("tag", keys);
        Assert.Contains("tags", keys);

        // Full roundtrip
        var reCompact = _codec.Encode("IntKeyedWithStringChild", named);
        var result = _codec.DecodeJson("IntKeyedWithStringChild", reCompact);
        Assert.Equal(5, result["id"]!.GetValue<int>());
        Assert.Equal("test", (result["tag"] as JsonObject)!["label"]!.GetValue<string?>());
        Assert.Single((result["tags"] as JsonArray)!);
    }

    [Fact]
    public void StringKeyedWithIntChild_Json_Roundtrip()
    {
        // string-keyed parent contains int-keyed IntKeyedItem children
        var obj = new StringKeyedWithIntChild
        {
            id = 2,
            child = new IntKeyedItem { id = 7, label = "intchild", value = 1.5 },
            children = new List<IntKeyedItem>
            {
                new() { id = 8, label = "c1", value = 2.0 },
                new() { id = 9, label = "c2", value = 3.0 }
            }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("StringKeyedWithIntChild", compact);

        Assert.Equal(2, json["id"]!.GetValue<int>());
        var child = json["child"] as JsonObject;
        Assert.Equal(7, child!["id"]!.GetValue<int>());
        Assert.Equal("intchild", child["label"]!.GetValue<string?>());
        var children = json["children"] as JsonArray;
        Assert.Equal(2, children!.Count);
        Assert.Equal(8, children[0]["id"]!.GetValue<int>());

        var reEncoded = _codec.EncodeJson("StringKeyedWithIntChild", json);
        var json2 = _codec.DecodeJson("StringKeyedWithIntChild", reEncoded);
        Assert.Equal(2, json2["id"]!.GetValue<int>());
        Assert.Equal(7, (json2["child"] as JsonObject)!["id"]!.GetValue<int>());
    }

    [Fact]
    public void StringKeyedWithIntChild_MsgPack_Roundtrip()
    {
        var obj = new StringKeyedWithIntChild
        {
            id = 3,
            child = new IntKeyedItem { id = 11, label = "deep", value = 9.9 },
            children = new List<IntKeyedItem>()
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("StringKeyedWithIntChild", compact);

        var reCompact = _codec.Encode("StringKeyedWithIntChild", named);
        var result = _codec.DecodeJson("StringKeyedWithIntChild", reCompact);
        Assert.Equal(3, result["id"]!.GetValue<int>());
        Assert.Equal("deep", (result["child"] as JsonObject)!["label"]!.GetValue<string?>());
        Assert.Empty((result["children"] as JsonArray)!);
    }

    [Fact]
    public void IntKeyedWithIntChild_Json_Roundtrip()
    {
        // int-keyed parent contains int-keyed children
        var obj = new IntKeyedWithIntChild
        {
            id = 4,
            child = new IntKeyedItem { id = 20, label = "nested", value = 7.7 },
            children = new List<IntKeyedItem>
            {
                new() { id = 21, label = "c1", value = 1.1 },
                new() { id = 22, label = "c2", value = 2.2 }
            }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("IntKeyedWithIntChild", compact);

        Assert.Equal(4, json["id"]!.GetValue<int>());
        var child = json["child"] as JsonObject;
        Assert.Equal(20, child!["id"]!.GetValue<int>());
        var children = json["children"] as JsonArray;
        Assert.Equal(2, children!.Count);
        Assert.Equal(21, children[0]["id"]!.GetValue<int>());

        var reEncoded = _codec.EncodeJson("IntKeyedWithIntChild", json);
        var json2 = _codec.DecodeJson("IntKeyedWithIntChild", reEncoded);
        Assert.Equal(20, (json2["child"] as JsonObject)!["id"]!.GetValue<int>());
    }

    [Fact]
    public void IntKeyedWithIntChild_MsgPack_Roundtrip()
    {
        var obj = new IntKeyedWithIntChild
        {
            id = 6,
            child = new IntKeyedItem { id = 30, label = "dbl-int", value = 0.5 },
            children = new List<IntKeyedItem> { new() { id = 31, label = "sub", value = 0.1 } }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("IntKeyedWithIntChild", compact);
        var reCompact = _codec.Encode("IntKeyedWithIntChild", named);
        var result = _codec.DecodeJson("IntKeyedWithIntChild", reCompact);

        Assert.Equal(6, result["id"]!.GetValue<int>());
        Assert.Equal(30, (result["child"] as JsonObject)!["id"]!.GetValue<int>());
        Assert.Single((result["children"] as JsonArray)!);
    }

    [Fact]
    public void IntKeyedWithIntChild_NullChild_Handled()
    {
        var obj = new IntKeyedWithIntChild { id = 7, child = null, children = new List<IntKeyedItem>() };
        var compact = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("IntKeyedWithIntChild", compact);
        Assert.Null(json["child"]);

        var named = _codec.Decode("IntKeyedWithIntChild", compact);
        var reCompact = _codec.Encode("IntKeyedWithIntChild", named);
        var result = _codec.DecodeJson("IntKeyedWithIntChild", reCompact);
        Assert.Null(result["child"]);
    }

    // ── Union type tests ─────────────────────────────────────────────────────

    [Fact]
    public void Union_StringKeyed_ChildA_Json_Roundtrip()
    {
        UnionBase child = new UnionChildA { label = "hello", value = 42 };
        var compact = MessagePackSerializer.Serialize(child);

        var json = _codec.DecodeJson("UnionBase", compact);
        Assert.Equal(0, json["_type"]!.GetValue<int>()); // UnionChildA = discriminator 0
        var data = json["_data"] as JsonObject;
        Assert.Equal("hello", data!["label"]!.GetValue<string?>());
        Assert.Equal(42, data["value"]!.GetValue<int>());

        var reEncoded = _codec.EncodeJson("UnionBase", json);
        var json2 = _codec.DecodeJson("UnionBase", reEncoded);
        Assert.Equal(0, json2["_type"]!.GetValue<int>());
        Assert.Equal("hello", (json2["_data"] as JsonObject)!["label"]!.GetValue<string?>());
        Assert.Equal(42, (json2["_data"] as JsonObject)!["value"]!.GetValue<int>());
    }

    [Fact]
    public void Union_StringKeyed_ChildB_Json_Roundtrip()
    {
        UnionBase child = new UnionChildB { message = "world", enabled = true };
        var compact = MessagePackSerializer.Serialize(child);

        var json = _codec.DecodeJson("UnionBase", compact);
        Assert.Equal(1, json["_type"]!.GetValue<int>()); // UnionChildB = discriminator 1
        var data = json["_data"] as JsonObject;
        Assert.Equal("world", data!["message"]!.GetValue<string?>());
        Assert.True(data["enabled"]!.GetValue<bool>());

        var reEncoded = _codec.EncodeJson("UnionBase", json);
        var json2 = _codec.DecodeJson("UnionBase", reEncoded);
        Assert.Equal(1, json2["_type"]!.GetValue<int>());
        Assert.True((json2["_data"] as JsonObject)!["enabled"]!.GetValue<bool>());
    }

    [Fact]
    public void Union_StringKeyed_ChildA_MsgPack_Roundtrip()
    {
        UnionBase child = new UnionChildA { label = "msgpack", value = 99 };
        var compact = MessagePackSerializer.Serialize(child);

        var named = _codec.Decode("UnionBase", compact);

        // Named format: {"_type": 0, "_data": {"label": ..., "value": ...}}
        var reader = new MessagePackReader(named);
        var mapCount = reader.ReadMapHeader();
        Assert.Equal(2, mapCount);
        var keys = new HashSet<string>();
        for (var i = 0; i < mapCount; i++)
        {
            keys.Add(reader.ReadString()!);
            reader.Skip();
        }

        Assert.Contains("_type", keys);
        Assert.Contains("_data", keys);

        var reCompact = _codec.Encode("UnionBase", named);
        var result = _codec.DecodeJson("UnionBase", reCompact);
        Assert.Equal(0, result["_type"]!.GetValue<int>());
        Assert.Equal("msgpack", (result["_data"] as JsonObject)!["label"]!.GetValue<string?>());
    }

    [Fact]
    public void Union_StringKeyed_ChildB_MsgPack_Roundtrip()
    {
        UnionBase child = new UnionChildB { message = "test", enabled = false };
        var compact = MessagePackSerializer.Serialize(child);

        var named = _codec.Decode("UnionBase", compact);
        var reCompact = _codec.Encode("UnionBase", named);
        var result = _codec.DecodeJson("UnionBase", reCompact);

        Assert.Equal(1, result["_type"]!.GetValue<int>());
        Assert.Equal("test", (result["_data"] as JsonObject)!["message"]!.GetValue<string?>());
        Assert.False((result["_data"] as JsonObject)!["enabled"]!.GetValue<bool>());
    }

    [Fact]
    public void Union_NullPayload_Handled()
    {
        // MessageWithUnion with null payload
        var msg = new MessageWithUnion { id = 1, payload = null, payloads = new List<UnionBase>() };
        var compact = MessagePackSerializer.Serialize(msg);

        var json = _codec.DecodeJson("MessageWithUnion", compact);
        Assert.Equal(1, json["id"]!.GetValue<int>());
        Assert.Null(json["payload"]);
        Assert.Empty((json["payloads"] as JsonArray)!);

        var named = _codec.Decode("MessageWithUnion", compact);
        var reCompact = _codec.Encode("MessageWithUnion", named);
        var result = _codec.DecodeJson("MessageWithUnion", reCompact);
        Assert.Null(result["payload"]);
    }

    [Fact]
    public void MessageWithUnion_AllVariants_Json_Roundtrip()
    {
        var msg = new MessageWithUnion
        {
            id = 42,
            payload = new UnionChildA { label = "main", value = 1 },
            payloads = new List<UnionBase>
            {
                new UnionChildA { label = "a", value = 10 },
                new UnionChildB { message = "b", enabled = true }
            }
        };
        var compact = MessagePackSerializer.Serialize(msg);
        var json = _codec.DecodeJson("MessageWithUnion", compact);

        Assert.Equal(42, json["id"]!.GetValue<int>());
        Assert.Equal(0, json["payload"]!["_type"]!.GetValue<int>());
        Assert.Equal("main", (json["payload"]!["_data"] as JsonObject)!["label"]!.GetValue<string?>());

        var payloads = json["payloads"] as JsonArray;
        Assert.Equal(2, payloads!.Count);
        Assert.Equal(0, payloads[0]["_type"]!.GetValue<int>());
        Assert.Equal(1, payloads[1]["_type"]!.GetValue<int>());
        Assert.Equal("a", (payloads[0]["_data"] as JsonObject)!["label"]!.GetValue<string?>());
        Assert.Equal("b", (payloads[1]["_data"] as JsonObject)!["message"]!.GetValue<string?>());

        // Roundtrip
        var reEncoded = _codec.EncodeJson("MessageWithUnion", json);
        var json2 = _codec.DecodeJson("MessageWithUnion", reEncoded);
        Assert.Equal(42, json2["id"]!.GetValue<int>());
        Assert.Equal("main", (json2["payload"]!["_data"] as JsonObject)!["label"]!.GetValue<string?>());
    }

    [Fact]
    public void MessageWithUnion_AllVariants_MsgPack_Roundtrip()
    {
        var msg = new MessageWithUnion
        {
            id = 7,
            payload = new UnionChildB { message = "hello", enabled = true },
            payloads = new List<UnionBase>
            {
                new UnionChildA { label = "x", value = 5 },
                new UnionChildB { message = "y", enabled = false }
            }
        };
        var compact = MessagePackSerializer.Serialize(msg);
        var named = _codec.Decode("MessageWithUnion", compact);
        var reCompact = _codec.Encode("MessageWithUnion", named);

        var result = _codec.DecodeJson("MessageWithUnion", reCompact);
        Assert.Equal(7, result["id"]!.GetValue<int>());
        Assert.Equal(1, result["payload"]!["_type"]!.GetValue<int>());
        Assert.Equal("hello", (result["payload"]!["_data"] as JsonObject)!["message"]!.GetValue<string?>());
        var payloads = result["payloads"] as JsonArray;
        Assert.Equal(2, payloads!.Count);
        Assert.Equal(0, payloads[0]["_type"]!.GetValue<int>());
        Assert.Equal(1, payloads[1]["_type"]!.GetValue<int>());
    }

    [Fact]
    public void Union_IntKeyed_ChildA_Json_Roundtrip()
    {
        IntUnionBase child = new IntUnionChildA { num = 77, name = "intunion" };
        var compact = MessagePackSerializer.Serialize(child);

        var json = _codec.DecodeJson("IntUnionBase", compact);
        Assert.Equal(0, json["_type"]!.GetValue<int>());
        var data = json["_data"] as JsonObject;
        Assert.Equal(77, data!["num"]!.GetValue<int>());
        Assert.Equal("intunion", data["name"]!.GetValue<string?>());

        var reEncoded = _codec.EncodeJson("IntUnionBase", json);
        var json2 = _codec.DecodeJson("IntUnionBase", reEncoded);
        Assert.Equal(77, (json2["_data"] as JsonObject)!["num"]!.GetValue<int>());
    }

    [Fact]
    public void Union_IntKeyed_ChildB_MsgPack_Roundtrip()
    {
        IntUnionBase child = new IntUnionChildB { data = "bytes", flag = true };
        var compact = MessagePackSerializer.Serialize(child);

        var named = _codec.Decode("IntUnionBase", compact);
        var reCompact = _codec.Encode("IntUnionBase", named);
        var result = _codec.DecodeJson("IntUnionBase", reCompact);

        Assert.Equal(1, result["_type"]!.GetValue<int>());
        Assert.Equal("bytes", (result["_data"] as JsonObject)!["data"]!.GetValue<string?>());
        Assert.True((result["_data"] as JsonObject)!["flag"]!.GetValue<bool>());
    }

    [Fact]
    public void IntKeyedWithUnion_AllVariants_Roundtrip()
    {
        var obj = new IntKeyedWithUnion
        {
            id = 100,
            payload = new IntUnionChildA { num = 1, name = "first" },
            payloads = new List<IntUnionBase>
            {
                new IntUnionChildB { data = "d", flag = true },
                new IntUnionChildA { num = 2, name = "second" }
            }
        };
        var compact = MessagePackSerializer.Serialize(obj);

        // JSON path
        var json = _codec.DecodeJson("IntKeyedWithUnion", compact);
        Assert.Equal(100, json["id"]!.GetValue<int>());
        Assert.Equal(0, json["payload"]!["_type"]!.GetValue<int>());
        Assert.Equal(1, (json["payload"]!["_data"] as JsonObject)!["num"]!.GetValue<int>());
        var payloads = json["payloads"] as JsonArray;
        Assert.Equal(2, payloads!.Count);
        Assert.Equal(1, payloads[0]["_type"]!.GetValue<int>());
        Assert.Equal(0, payloads[1]["_type"]!.GetValue<int>());

        // MsgPack path
        var named = _codec.Decode("IntKeyedWithUnion", compact);
        var reCompact = _codec.Encode("IntKeyedWithUnion", named);
        var result = _codec.DecodeJson("IntKeyedWithUnion", reCompact);
        Assert.Equal(100, result["id"]!.GetValue<int>());
        Assert.Equal(1, (result["payload"]!["_data"] as JsonObject)!["num"]!.GetValue<int>());
    }

    [Fact]
    public void IntKeyedWithUnion_NullPayload_Roundtrip()
    {
        var obj = new IntKeyedWithUnion { id = 5, payload = null, payloads = new List<IntUnionBase>() };
        var compact = MessagePackSerializer.Serialize(obj);

        var json = _codec.DecodeJson("IntKeyedWithUnion", compact);
        Assert.Null(json["payload"]);

        var named = _codec.Decode("IntKeyedWithUnion", compact);
        var reCompact = _codec.Encode("IntKeyedWithUnion", named);
        var result = _codec.DecodeJson("IntKeyedWithUnion", reCompact);
        Assert.Null(result["payload"]);
    }
}