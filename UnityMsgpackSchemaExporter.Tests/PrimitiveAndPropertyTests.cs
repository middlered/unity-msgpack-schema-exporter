using System.Text.Json;
using System.Text.Json.Nodes;
using MessagePack;
using UnityMsgpackSchemaExporter.TestSchemas;

namespace UnityMsgpackSchemaExporter.Tests;

/// <summary>
///     Tests for all primitive types, property-keyed schemas, byte[] handling,
///     and WritePrimitive default branches.
/// </summary>
public class PrimitiveAndPropertyTests : IDisposable
{
    private readonly MsgPackCodec _codec;
    private readonly SchemaExtractor _extractor;

    public PrimitiveAndPropertyTests()
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
    // AllPrimitiveTypes: full roundtrip via JSON
    // =========================================
    [Fact]
    public void AllPrimitives_Json_Roundtrip()
    {
        var obj = new AllPrimitiveTypes
        {
            byteVal = 0xFF,
            sbyteVal = -100,
            shortVal = -30000,
            ushortVal = 60000,
            intVal = -2000000,
            uintVal = 4000000000,
            longVal = -9000000000000L,
            ulongVal = 18000000000000000000UL,
            floatVal = 3.14f,
            doubleVal = 2.71828,
            boolVal = true,
            stringVal = "hello",
            bytesVal = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }
        };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);

        Assert.Equal(0xFF, json["byteVal"]!.GetValue<int>());
        Assert.Equal(-100, json["sbyteVal"]!.GetValue<int>());
        Assert.Equal(-30000, json["shortVal"]!.GetValue<int>());
        Assert.Equal(60000, json["ushortVal"]!.GetValue<int>());
        Assert.Equal(-2000000, json["intVal"]!.GetValue<int>());
        Assert.Equal(4000000000U, json["uintVal"]!.GetValue<uint>());
        Assert.Equal(-9000000000000L, json["longVal"]!.GetValue<long>());
        Assert.Equal(18000000000000000000UL, json["ulongVal"]!.GetValue<ulong>());
        Assert.True(Math.Abs(json["floatVal"]!.GetValue<float>() - 3.14f) < 0.01f);
        Assert.True(Math.Abs(json["doubleVal"]!.GetValue<double>() - 2.71828) < 0.0001);
        Assert.True(json["boolVal"]!.GetValue<bool>());
        Assert.Equal("hello", json["stringVal"]!.GetValue<string?>());
        // byte[] is encoded as base64
        Assert.NotNull(json["bytesVal"]);
        var bytesBase64 = json["bytesVal"]!.GetValue<string?>();
        Assert.Equal(Convert.ToBase64String(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }), bytesBase64);
    }

    [Fact]
    public void AllPrimitives_Json_EncodeRoundtrip()
    {
        var obj = new AllPrimitiveTypes
        {
            byteVal = 42,
            sbyteVal = -1,
            shortVal = 100,
            ushortVal = 200,
            intVal = 300,
            uintVal = 400,
            longVal = 500,
            ulongVal = 600,
            floatVal = 1.5f,
            doubleVal = 2.5,
            boolVal = false,
            stringVal = "test",
            bytesVal = new byte[] { 1, 2, 3 }
        };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);

        Assert.Equal(42, decoded["byteVal"]!.GetValue<int>());
        Assert.Equal(-1, decoded["sbyteVal"]!.GetValue<int>());
        Assert.Equal(100, decoded["shortVal"]!.GetValue<int>());
        Assert.Equal(200, decoded["ushortVal"]!.GetValue<int>());
        Assert.Equal(300, decoded["intVal"]!.GetValue<int>());
        Assert.Equal(400U, decoded["uintVal"]!.GetValue<uint>());
        Assert.Equal(500L, decoded["longVal"]!.GetValue<long>());
        Assert.Equal(600UL, decoded["ulongVal"]!.GetValue<ulong>());
        Assert.True(!decoded["boolVal"]!.GetValue<bool>());
        Assert.Equal("test", decoded["stringVal"]!.GetValue<string?>());
    }

    [Fact]
    public void AllPrimitives_NullBytes_Roundtrip()
    {
        var obj = new AllPrimitiveTypes
        {
            byteVal = 0,
            stringVal = "x",
            bytesVal = null
        };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        Assert.Null(json["bytesVal"]);

        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Null(decoded["bytesVal"]);
    }

    [Fact]
    public void AllPrimitives_NullString_Roundtrip()
    {
        var obj = new AllPrimitiveTypes { stringVal = null! };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        Assert.Null(json["stringVal"]);
    }

    // =========================================
    // AllPrimitiveTypes: MsgPack transcode
    // =========================================
    [Fact]
    public void AllPrimitives_MsgPack_Transcode()
    {
        var obj = new AllPrimitiveTypes
        {
            byteVal = 42,
            sbyteVal = -1,
            shortVal = 100,
            ushortVal = 200,
            intVal = 300,
            uintVal = 400,
            longVal = 500,
            ulongVal = 600,
            floatVal = 1.5f,
            doubleVal = 2.5,
            boolVal = true,
            stringVal = "test",
            bytesVal = new byte[] { 0xAA, 0xBB }
        };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("AllPrimitiveTypes", compact);
        var recompact = _codec.Encode("AllPrimitiveTypes", named);
        var json = _codec.DecodeJson("AllPrimitiveTypes", recompact);

        Assert.Equal(42, json["byteVal"]!.GetValue<int>());
        Assert.True(json["boolVal"]!.GetValue<bool>());
        Assert.Equal("test", json["stringVal"]!.GetValue<string?>());
    }

    // =========================================
    // PropertyKeyed: schema extraction
    // =========================================
    [Fact]
    public void PropertyKeyed_Extracted()
    {
        var schema = _extractor.FindSchema("PropertyKeyed");
        Assert.NotNull(schema);
        Assert.True(schema!.IsStringKeyed);
        Assert.Equal(3, schema.Fields.Count);
        Assert.True(schema.Fields.Any(f => f.IsProperty));
    }

    [Fact]
    public void PropertyKeyed_Json_Roundtrip()
    {
        var obj = new PropertyKeyed { Id = 1, Name = "test", Active = true };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("PropertyKeyed", data);
        Assert.Equal(1, json["id"]!.GetValue<int>());
        Assert.Equal("test", json["name"]!.GetValue<string?>());
        Assert.True(json["active"]!.GetValue<bool>());

        var encoded = _codec.EncodeJson("PropertyKeyed", json);
        var decoded = _codec.DecodeJson("PropertyKeyed", encoded);
        Assert.Equal(1, decoded["id"]!.GetValue<int>());
    }

    [Fact]
    public void PropertyKeyed_MsgPack_Roundtrip()
    {
        var obj = new PropertyKeyed { Id = 1, Name = "test", Active = true };
        var compact = MessagePackSerializer.Serialize(obj);
        var named = _codec.Decode("PropertyKeyed", compact);
        var recompact = _codec.Encode("PropertyKeyed", named);
        var json = _codec.DecodeJson("PropertyKeyed", recompact);
        Assert.Equal(1, json["id"]!.GetValue<int>());
    }

    // =========================================
    // MixedFieldsAndProperties: schema extraction
    // =========================================
    [Fact]
    public void MixedFieldsAndProperties_Extracted()
    {
        var schema = _extractor.FindSchema("MixedFieldsAndProperties");
        Assert.NotNull(schema);
        Assert.False(schema!.IsStringKeyed);
        Assert.Equal(2, schema.Fields.Count);
        Assert.True(schema.Fields.Any(f => f.IsProperty));
        Assert.True(schema.Fields.Any(f => !f.IsProperty));
    }

    [Fact]
    public void MixedFieldsAndProperties_Json_Roundtrip()
    {
        var obj = new MixedFieldsAndProperties { fieldValue = 42, PropertyValue = "hello" };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("MixedFieldsAndProperties", data);
        Assert.Equal(42, json["fieldValue"]!.GetValue<int>());
        Assert.Equal("hello", json["PropertyValue"]!.GetValue<string?>());
    }

    // =========================================
    // AllPrimitiveTypes: Avro export
    // =========================================
    [Fact]
    public void AllPrimitives_Avro_Export()
    {
        var schema = _extractor.FindSchema("AllPrimitiveTypes")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        Assert.NotNull(fields);

        // byte -> int
        var byteField = fields!.First(f => f["name"]!.ToString() == "byteVal");
        Assert.Equal("int", byteField["type"]!.ToString());

        // double -> double
        var doubleField = fields!.First(f => f["name"]!.ToString() == "doubleVal");
        Assert.Equal("double", doubleField["type"]!.ToString());

        // byte[] -> bytes
        var bytesField = fields!.First(f => f["name"]!.ToString() == "bytesVal");
        Assert.Equal("bytes", bytesField["type"]!.ToString());

        // uint -> long (Avro doesn't have uint)
        var uintField = fields!.First(f => f["name"]!.ToString() == "uintVal");
        Assert.Equal("long", uintField["type"]!.ToString());
    }

    // =========================================
    // Avro: WrapNullable for "null" itself
    // =========================================
    [Fact]
    public void Avro_AllNullable_NullStringField_NotDoubleWrapped()
    {
        var schema = _extractor.FindSchema("AllPrimitiveTypes")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas, new AvroExportOptions { AllNullable = true });
        var fields = avro["fields"] as JsonArray;
        var stringField = fields!.First(f => f["name"]!.ToString() == "stringVal");
        // string is already ["null", "string"], should not be double-wrapped
        var type = stringField["type"] as JsonArray;
        Assert.NotNull(type);
        var nullCount = type!.Count(t => t?.GetValueKind() == JsonValueKind.String && t.GetValue<string>() == "null");
        Assert.Equal(1, nullCount);
    }

    // =========================================
    // WritePrimitive: byte[] null encode
    // =========================================
    [Fact]
    public void WritePrimitive_ByteArray_Null()
    {
        var json = new JsonObject
        {
            ["byteVal"] = 0,
            ["sbyteVal"] = 0,
            ["shortVal"] = 0,
            ["ushortVal"] = 0,
            ["intVal"] = 0,
            ["uintVal"] = 0,
            ["longVal"] = 0,
            ["ulongVal"] = 0,
            ["floatVal"] = 0,
            ["doubleVal"] = 0,
            ["boolVal"] = false,
            ["stringVal"] = null,
            ["bytesVal"] = null
        };
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Null(decoded["bytesVal"]);
        Assert.Null(decoded["stringVal"]);
    }

    // =========================================
    // WritePrimitive: byte[] with base64 value
    // =========================================
    [Fact]
    public void WritePrimitive_ByteArray_Base64()
    {
        var b64 = Convert.ToBase64String(new byte[] { 1, 2, 3, 4 });
        var json = new JsonObject
        {
            ["byteVal"] = 0,
            ["sbyteVal"] = 0,
            ["shortVal"] = 0,
            ["ushortVal"] = 0,
            ["intVal"] = 0,
            ["uintVal"] = 0,
            ["longVal"] = 0,
            ["ulongVal"] = 0,
            ["floatVal"] = 0,
            ["doubleVal"] = 0,
            ["boolVal"] = false,
            ["stringVal"] = "x",
            ["bytesVal"] = b64
        };
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Equal(b64, decoded["bytesVal"]!.GetValue<string?>());
    }

    // =========================================
    // ReadPrimitive: individual type coverage
    // =========================================
    [Fact]
    public void ReadPrimitive_Uint()
    {
        var obj = new AllPrimitiveTypes { uintVal = uint.MaxValue, stringVal = "x" };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        Assert.Equal(uint.MaxValue, json["uintVal"]!.GetValue<uint>());
    }

    [Fact]
    public void ReadPrimitive_Ulong()
    {
        var obj = new AllPrimitiveTypes { ulongVal = ulong.MaxValue, stringVal = "x" };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        Assert.Equal(ulong.MaxValue, json["ulongVal"]!.GetValue<ulong>());
    }

    [Fact]
    public void ReadPrimitive_Float()
    {
        var obj = new AllPrimitiveTypes { floatVal = float.MaxValue, stringVal = "x" };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        Assert.True(Math.Abs(json["floatVal"]!.GetValue<float>() - float.MaxValue) / float.MaxValue < 0.001);
    }

    [Fact]
    public void ReadPrimitive_Double()
    {
        var obj = new AllPrimitiveTypes { doubleVal = double.MaxValue, stringVal = "x" };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        Assert.True(Math.Abs(json["doubleVal"]!.GetValue<double>() - double.MaxValue) / double.MaxValue < 0.001);
    }

    [Fact]
    public void ReadPrimitive_Bool()
    {
        var obj = new AllPrimitiveTypes { boolVal = true, stringVal = "x" };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        Assert.True(json["boolVal"]!.GetValue<bool>());
    }

    [Fact]
    public void ReadPrimitive_Byte()
    {
        var obj = new AllPrimitiveTypes { byteVal = 255, stringVal = "x" };
        var data = MessagePackSerializer.Serialize(obj);
        var json = _codec.DecodeJson("AllPrimitiveTypes", data);
        Assert.Equal(255, json["byteVal"]!.GetValue<int>());
    }

    // =========================================
    // WritePrimitive: uint, ulong, byte, bool, float, double
    // =========================================
    [Fact]
    public void WritePrimitive_Uint_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["uintVal"] = uint.MaxValue;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Equal(uint.MaxValue, decoded["uintVal"]!.GetValue<uint>());
    }

    [Fact]
    public void WritePrimitive_Ulong_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["ulongVal"] = ulong.MaxValue;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Equal(ulong.MaxValue, decoded["ulongVal"]!.GetValue<ulong>());
    }

    [Fact]
    public void WritePrimitive_Byte_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["byteVal"] = 200;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Equal(200, decoded["byteVal"]!.GetValue<int>());
    }

    [Fact]
    public void WritePrimitive_Bool_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["boolVal"] = true;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.True(decoded["boolVal"]!.GetValue<bool>());
    }

    [Fact]
    public void WritePrimitive_Float_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["floatVal"] = 1.23f;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.True(Math.Abs(decoded["floatVal"]!.GetValue<float>() - 1.23f) < 0.01f);
    }

    [Fact]
    public void WritePrimitive_Double_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["doubleVal"] = 9.87654321;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.True(Math.Abs(decoded["doubleVal"]!.GetValue<double>() - 9.87654321) < 0.0001);
    }

    [Fact]
    public void WritePrimitive_Short_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["shortVal"] = -100;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Equal(-100, decoded["shortVal"]!.GetValue<int>());
    }

    [Fact]
    public void WritePrimitive_Ushort_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["ushortVal"] = 50000;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Equal(50000, decoded["ushortVal"]!.GetValue<int>());
    }

    [Fact]
    public void WritePrimitive_Long_Encode()
    {
        var json = MakeAllPrimitivesJson();
        json["longVal"] = long.MinValue;
        var encoded = _codec.EncodeJson("AllPrimitiveTypes", json);
        var decoded = _codec.DecodeJson("AllPrimitiveTypes", encoded);
        Assert.Equal(long.MinValue, decoded["longVal"]!.GetValue<long>());
    }

    private static JsonObject MakeAllPrimitivesJson()
    {
        return new JsonObject
        {
            ["byteVal"] = 0,
            ["sbyteVal"] = 0,
            ["shortVal"] = 0,
            ["ushortVal"] = 0,
            ["intVal"] = 0,
            ["uintVal"] = 0,
            ["longVal"] = 0,
            ["ulongVal"] = 0,
            ["floatVal"] = 0,
            ["doubleVal"] = 0,
            ["boolVal"] = false,
            ["stringVal"] = "",
            ["bytesVal"] = Convert.ToBase64String(Array.Empty<byte>())
        };
    }
}