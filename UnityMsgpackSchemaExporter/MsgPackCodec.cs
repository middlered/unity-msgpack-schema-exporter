using System.Buffers;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using MessagePack;

namespace UnityMsgpackSchemaExporter;

/// <summary>
///     Codec for converting between named-key JSON / MessagePack and the compact
///     keyless MessagePack binary format used by MessagePack-CSharp.
/// </summary>
public class MsgPackCodec(IReadOnlyDictionary<string, MsgPackClassSchema> schemas)
{
    private readonly IReadOnlyDictionary<string, MsgPackClassSchema> _schemas = schemas;

    /// <summary>
    ///     Decode compact MessagePack binary to a named-key JSON token.
    /// </summary>
    public JsonNode? DecodeJson(string className, byte[] data)
    {
        var schema = FindSchema(className);
        var reader = new MessagePackReader(data);
        return DecodeValue(ref reader, schema);
    }

    /// <summary>
    ///     Encode a named-key JSON token to compact MessagePack binary.
    /// </summary>
    public byte[] EncodeJson(string className, JsonNode? json)
    {
        var schema = FindSchema(className);
        var writer = new ArrayBufferWriter();
        var mpWriter = new MessagePackWriter(writer);
        EncodeValue(ref mpWriter, json, schema);
        mpWriter.Flush();
        return writer.ToArray();
    }

    /// <summary>
    ///     Decode compact MessagePack binary to named-key MessagePack (string-keyed map).
    ///     Reads the compact format (array/int-keyed or map/string-keyed) produced by
    ///     MessagePack-CSharp and writes a standard msgpack map whose keys are the C#
    ///     member names.  Nested schema types are transcoded recursively; primitive
    ///     values and dictionary keys are copied verbatim (no type conversion).
    /// </summary>
    public byte[] Decode(string className, byte[] data)
    {
        var schema = FindSchema(className);
        var reader = new MessagePackReader(data);
        var buf = new ArrayBufferWriter();
        var mpw = new MessagePackWriter(buf);
        TranscodeToNamed(ref reader, ref mpw, schema);
        mpw.Flush();
        return buf.ToArray();
    }

    /// <summary>
    ///     Encode named-key MessagePack (string-keyed map) to compact MessagePack binary.
    ///     Input is a msgpack map whose keys are C# member names (as produced by
    ///     <see cref="Decode" /> or any msgpack library).  Output is the compact
    ///     indexed/keyless format expected by MessagePack-CSharp.
    /// </summary>
    public byte[] Encode(string className, byte[] data)
    {
        var schema = FindSchema(className);
        var reader = new MessagePackReader(data);
        var buf = new ArrayBufferWriter();
        var mpw = new MessagePackWriter(buf);
        TranscodeToCompact(ref reader, ref mpw, schema);
        mpw.Flush();
        return buf.ToArray();
    }

    private JsonNode? DecodeValue(ref MessagePackReader reader, MsgPackClassSchema schema)
    {
        if (reader.TryReadNil()) return null;

        // Union base: wire format is [discriminator, payload]
        if (schema.Unions.Count > 0) return DecodeUnionPayload(ref reader, schema);

        var obj = new JsonObject();

        if (schema.IsStringKeyed)
        {
            // String-keyed: MessagePack map format {key: value, ...}
            var mapCount = reader.ReadMapHeader();
            var fieldLookup = schema.Fields.ToDictionary(f => f.StringKey ?? f.MemberName);

            for (var i = 0; i < mapCount; i++)
            {
                var key = reader.ReadString();
                if (key != null && fieldLookup.TryGetValue(key, out var field))
                {
                    obj[key] = DecodeFieldValue(ref reader, field.TypeName);
                }
                else
                    // Unknown key - read and store as-is
                {
                    obj[key ?? $"_unknown_{i}"] = ReadAny(ref reader);
                }
            }
        }
        else
        {
            // Integer-keyed: MessagePack array format [val0, val1, ...]
            var arrCount = reader.ReadArrayHeader();
            var fieldByIndex = schema.Fields.ToDictionary(f => f.IntKey);
            var maxKey = schema.Fields.Count > 0 ? schema.Fields.Max(f => f.IntKey) : -1;

            for (var i = 0; i < arrCount; i++)
            {
                if (fieldByIndex.TryGetValue(i, out var field))
                {
                    var keyName = field.StringKey ?? field.MemberName;
                    obj[keyName] = DecodeFieldValue(ref reader, field.TypeName);
                }
                else
                {
                    obj[$"_index_{i}"] = ReadAny(ref reader);
                }
            }
        }

        return obj;
    }

    private JsonNode? DecodeFieldValue(ref MessagePackReader reader, string typeName)
    {
        if (reader.TryReadNil())
        {
            return null;
        }

        // Handle nullable
        if (typeName.StartsWith("Nullable<"))
        {
            var inner = typeName[9..^1];
            return DecodeFieldValue(ref reader, inner);
        }

        // Handle byte[] specially — msgpack serializes as binary, not array
        if (typeName == "byte[]")
        {
            return DecodeBytesField(ref reader);
        }

        // Handle arrays
        if (typeName.EndsWith("[]"))
        {
            var elemType = typeName[..^2];
            return DecodeArray(ref reader, elemType);
        }

        // Handle List<T>
        if (typeName.StartsWith("List<") && typeName.EndsWith(">"))
        {
            var elemType = typeName[5..^1];
            return DecodeArray(ref reader, elemType);
        }

        // Handle HashSet<T>
        if (typeName.StartsWith("HashSet<") && typeName.EndsWith(">"))
        {
            var elemType = typeName[8..^1];
            return DecodeArray(ref reader, elemType);
        }

        // Handle Dictionary<K,V>
        if (typeName.StartsWith("Dictionary<"))
        {
            var inner = typeName[11..^1];
            return DecodeMap(ref reader, inner);
        }

        // Check if this is a known MessagePack object
        var nested = FindSchemaByTypeName(typeName);
        if (nested != null)
        {
            if (nested.Unions.Count > 0) return DecodeUnionPayload(ref reader, nested);
            return DecodeValue(ref reader, nested);
        }

        return ReadPrimitive(ref reader, typeName);
    }

    /// <summary>
    ///     Decode a Union value: wire format is [discriminator, payload].
    ///     Returns {"_type": discriminator, "_data": {named-key fields}}.
    /// </summary>
    private JsonNode? DecodeUnionPayload(ref MessagePackReader reader, MsgPackClassSchema unionSchema)
    {
        reader.ReadArrayHeader(); // 2: [discriminator, payload]
        var discriminator = reader.ReadInt32();
        var entry = unionSchema.Unions.FirstOrDefault(u => u.Key == discriminator);
        var concreteSchema = entry != null ? FindSchemaByTypeName(entry.TypeName) : null;
        return new JsonObject
        {
            ["_type"] = discriminator,
            ["_data"] = concreteSchema != null ? DecodeValue(ref reader, concreteSchema) : ReadAny(ref reader)
        };
    }

    private JsonArray DecodeArray(ref MessagePackReader reader, string elemType)
    {
        if (reader.TryReadNil()) return new JsonArray();

        var count = reader.ReadArrayHeader();
        var arr = new JsonArray();
        for (var i = 0; i < count; i++)
        {
            arr.Add(DecodeFieldValue(ref reader, elemType));
        }

        return arr;
    }

    private JsonNode? DecodeMap(ref MessagePackReader reader, string genericArgs)
    {
        if (reader.TryReadNil()) return null;
        var count = reader.ReadMapHeader();

        var (keyType, valType) = SplitGenericArgs(genericArgs);
        var obj = new JsonObject();
        for (var i = 0; i < count; i++)
        {
            var keyNode = ReadPrimitive(ref reader, keyType);
            var key = NodeToString(keyNode, keyType);
            var val = DecodeFieldValue(ref reader, valType);
            obj[key] = val;
        }

        return obj;
    }

    private static string NodeToString(JsonNode? node, string typeName)
    {
        if (node == null) return "null";
        return typeName == "string" ? node.GetValue<string?>() ?? "null" : node.ToJsonString();
    }

    private JsonNode? ReadPrimitive(ref MessagePackReader reader, string typeName)
    {
        return typeName switch
        {
            "int" or "short" or "sbyte" => JsonValue.Create(reader.ReadInt32()),
            "ushort" => JsonValue.Create((int)reader.ReadUInt16()), // Store as int for compatibility
            "uint" => JsonValue.Create(reader.ReadUInt32()),
            "long" => JsonValue.Create(reader.ReadInt64()),
            "ulong" => JsonValue.Create(reader.ReadUInt64()),
            "float" => JsonValue.Create(reader.ReadSingle()),
            "double" => JsonValue.Create(reader.ReadDouble()),
            "bool" => JsonValue.Create(reader.ReadBoolean()),
            "byte" => JsonValue.Create((int)reader.ReadByte()), // Store as int for easier GetValue<int>()
            "string" => reader.TryReadNil() ? null : JsonValue.Create(reader.ReadString()),
            "byte[]" => reader.TryReadNil() ? null : DecodeBytesField(ref reader),
            _ => ReadAny(ref reader) // enum or unknown -> try reading as-is
        };
    }

    private static byte[] SeqToArray(ReadOnlySequence<byte> seq)
    {
        var buf = new byte[seq.Length];
        seq.CopyTo(buf);
        return buf;
    }

    private static JsonNode? DecodeBytesField(ref MessagePackReader reader)
    {
        var seq = reader.ReadBytes();
        var bytes = seq.HasValue ? SeqToArray(seq.Value) : Array.Empty<byte>();
        return JsonValue.Create(Convert.ToBase64String(bytes));
    }

    private static JsonNode? ReadAny(ref MessagePackReader reader)
    {
        var type = reader.NextMessagePackType;
        if (reader.End) return null;

        switch (type)
        {
            case MessagePackType.Nil:
                reader.ReadNil();
                return null;
            case MessagePackType.Boolean:
                return JsonValue.Create(reader.ReadBoolean());
            case MessagePackType.Integer:
                // Try to read as the smallest type that fits
                // MessagePack library throws on overflow, so we need to try each type
                var saved = reader.CreatePeekReader();
                try
                {
                    var intVal = saved.ReadInt32();
                    reader.ReadInt32(); // Consume if successful
                    return JsonValue.Create(intVal);
                }
                catch
                {
                    try
                    {
                        var longVal = reader.ReadInt64();
                        return JsonValue.Create(longVal);
                    }
                    catch
                    {
                        return JsonValue.Create(reader.ReadUInt64());
                    }
                }
            case MessagePackType.Float:
                return JsonValue.Create(reader.ReadDouble());
            case MessagePackType.String:
                return JsonValue.Create(reader.ReadString());
            case MessagePackType.Binary:
                var seq2 = reader.ReadBytes();
                return seq2.HasValue ? JsonValue.Create(Convert.ToBase64String(SeqToArray(seq2.Value))) : null;

            case MessagePackType.Array:
                var arrLen = reader.ReadArrayHeader();
                var arr = new JsonArray();
                for (var i = 0; i < arrLen; i++)
                {
                    arr.Add(ReadAny(ref reader));
                }

                return arr;
            case MessagePackType.Map:
                var mapLen = reader.ReadMapHeader();
                var obj = new JsonObject();
                for (var i = 0; i < mapLen; i++)
                {
                    var keyNode = ReadAny(ref reader);
                    var key = keyNode?.ToJsonString() ?? "null";
                    // Remove quotes from string keys
                    if (key.StartsWith('"') && key.EndsWith('"'))
                    {
                        key = key[1..^1];
                    }

                    var val = ReadAny(ref reader);
                    obj[key] = val;
                }

                return obj;
            case MessagePackType.Extension:
                var ext = reader.ReadExtensionFormat();
                var extData = new byte[ext.Data.Length];
                ext.Data.CopyTo(extData);
                return new JsonObject
                {
                    ["_ext_type"] = (int)ext.TypeCode, // Store as int for easier GetValue<int>()
                    ["_ext_data"] = Convert.ToBase64String(extData)
                };
            case MessagePackType.Unknown:
            default:
                reader.Skip();
                return null;
        }
    }

    private void EncodeValue(ref MessagePackWriter writer, JsonNode? json, MsgPackClassSchema schema)
    {
        if (json == null)
        {
            writer.WriteNil();
            return;
        }

        // Union base: write [discriminator, payload]
        if (schema.Unions.Count > 0)
        {
            EncodeUnionPayload(ref writer, json, schema);
            return;
        }

        if (json is not JsonObject obj)
        {
            writer.WriteNil();
            return;
        }

        if (schema.IsStringKeyed)
        {
            // Count how many fields we'll write
            var fieldsToWrite = schema.Fields
                .Where(f => obj.ContainsKey(f.StringKey ?? f.MemberName))
                .ToList();

            writer.WriteMapHeader(fieldsToWrite.Count);

            foreach (var field in fieldsToWrite)
            {
                var key = field.StringKey ?? field.MemberName;
                writer.Write(key);
                EncodeFieldValue(ref writer, obj[key], field.TypeName);
            }
        }
        else
        {
            // Integer-keyed: write as array up to max index
            var maxKey = schema.Fields.Count > 0 ? schema.Fields.Max(f => f.IntKey) : -1;
            writer.WriteArrayHeader(maxKey + 1);

            var fieldByIndex = schema.Fields.ToDictionary(f => f.IntKey);
            for (var i = 0; i <= maxKey; i++)
            {
                if (fieldByIndex.TryGetValue(i, out var field))
                {
                    var key = field.StringKey ?? field.MemberName;
                    if (obj.TryGetPropertyValue(key, out var value))
                    {
                        EncodeFieldValue(ref writer, value, field.TypeName);
                    }
                    else
                    {
                        writer.WriteNil();
                    }
                }
                else
                {
                    writer.WriteNil();
                }
            }
        }
    }

    private void EncodeFieldValue(ref MessagePackWriter writer, JsonNode? value, string typeName)
    {
        if (value == null)
        {
            writer.WriteNil();
            return;
        }

        // Handle nullable
        if (typeName.StartsWith("Nullable<"))
        {
            var inner = typeName[9..^1];
            EncodeFieldValue(ref writer, value, inner);
            return;
        }

        // Handle byte[] specially — msgpack serializes as binary, not array
        if (typeName == "byte[]")
        {
            WritePrimitive(ref writer, value, typeName);
            return;
        }

        // Handle arrays
        if (typeName.EndsWith("[]"))
        {
            var elemType = typeName[..^2];
            EncodeArray(ref writer, value, elemType);
            return;
        }

        // Handle List<T>
        if (typeName.StartsWith("List<") && typeName.EndsWith(">"))
        {
            var elemType = typeName[5..^1];
            EncodeArray(ref writer, value, elemType);
            return;
        }

        // Handle HashSet<T>
        if (typeName.StartsWith("HashSet<") && typeName.EndsWith(">"))
        {
            var elemType = typeName[8..^1];
            EncodeArray(ref writer, value, elemType);
            return;
        }

        // Handle Dictionary<K,V>
        if (typeName.StartsWith("Dictionary<"))
        {
            var inner = typeName[11..^1];
            EncodeMap(ref writer, value, inner);
            return;
        }

        // Check if this is a known MessagePack object
        var nested = FindSchemaByTypeName(typeName);
        if (nested != null)
        {
            if (nested.Unions.Count > 0)
            {
                EncodeUnionPayload(ref writer, value, nested);
                return;
            }

            EncodeValue(ref writer, value, nested);
            return;
        }

        // Primitive types
        WritePrimitive(ref writer, value, typeName);
    }

    /// <summary>
    ///     Encode a Union value: writes [discriminator, payload].
    ///     Input must be {"_type": discriminator, "_data": {concrete fields}}.
    /// </summary>
    private void EncodeUnionPayload(ref MessagePackWriter writer, JsonNode? json, MsgPackClassSchema unionSchema)
    {
        if (json is not JsonObject obj)
        {
            writer.WriteNil();
            return;
        }

        var discriminator = obj["_type"]?.GetValue<int>() ?? 0;
        var entry = unionSchema.Unions.FirstOrDefault(u => u.Key == discriminator);
        var concreteSchema = entry != null ? FindSchemaByTypeName(entry.TypeName) : null;
        writer.WriteArrayHeader(2);
        writer.Write(discriminator);
        if (concreteSchema != null && obj.TryGetPropertyValue("_data", out var dataNode))
        {
            EncodeValue(ref writer, dataNode, concreteSchema);
        }
        else
        {
            writer.WriteNil();
        }
    }

    private void EncodeArray(ref MessagePackWriter writer, JsonNode? value, string elemType)
    {
        if (value is not JsonArray arr)
        {
            writer.WriteNil();
            return;
        }

        writer.WriteArrayHeader(arr.Count);
        foreach (var item in arr)
        {
            EncodeFieldValue(ref writer, item, elemType);
        }
    }

    private void EncodeMap(ref MessagePackWriter writer, JsonNode? value, string genericArgs)
    {
        if (value is not JsonObject obj)
        {
            writer.WriteNil();
            return;
        }

        var (keyType, valType) = SplitGenericArgs(genericArgs);
        writer.WriteMapHeader(obj.Count);
        foreach (var (key, val) in obj)
        {
            WriteStringKeyAsPrimitive(ref writer, key, keyType);
            EncodeFieldValue(ref writer, val, valType);
        }
    }

    private static void WriteStringKeyAsPrimitive(ref MessagePackWriter writer, string strKey, string typeName)
    {
        switch (typeName)
        {
            case "int" or "short" or "sbyte": writer.Write(int.Parse(strKey, CultureInfo.InvariantCulture)); break;
            case "uint" or "ushort": writer.Write(uint.Parse(strKey, CultureInfo.InvariantCulture)); break;
            case "long": writer.Write(long.Parse(strKey, CultureInfo.InvariantCulture)); break;
            case "ulong": writer.Write(ulong.Parse(strKey, CultureInfo.InvariantCulture)); break;
            case "float": writer.Write(float.Parse(strKey, CultureInfo.InvariantCulture)); break;
            case "double": writer.Write(double.Parse(strKey, CultureInfo.InvariantCulture)); break;
            case "bool": writer.Write(bool.Parse(strKey)); break;
            case "byte": writer.Write(byte.Parse(strKey, CultureInfo.InvariantCulture)); break;
            default: writer.Write(strKey); break;
        }
    }

    private static void WritePrimitive(ref MessagePackWriter writer, JsonNode? value, string typeName)
    {
        switch (typeName)
        {
            case "int" or "short" or "sbyte":
                writer.Write(GetJsonInt32(value));
                break;
            case "uint" or "ushort":
                writer.Write(GetJsonUInt32(value));
                break;
            case "long":
                writer.Write(GetJsonInt64(value));
                break;
            case "ulong":
                writer.Write(GetJsonUInt64(value));
                break;
            case "float":
                writer.Write(GetJsonSingle(value));
                break;
            case "double":
                writer.Write(GetJsonDouble(value));
                break;
            case "bool":
                writer.Write(value!.GetValue<bool>());
                break;
            case "byte":
                writer.Write((byte)GetJsonInt32(value));
                break;
            case "string":
                if (value == null)
                {
                    writer.WriteNil();
                }
                else
                {
                    writer.Write(value.GetValue<string?>());
                }

                break;
            case "byte[]":
                if (value == null)
                {
                    writer.WriteNil();
                }
                else
                {
                    writer.Write(Convert.FromBase64String(value.GetValue<string>()));
                }

                break;
            default:
                if (value == null)
                {
                    writer.WriteNil();
                    break;
                }

                switch (value.GetValueKind())
                {
                    case JsonValueKind.Number:
                        try
                        {
                            writer.Write(GetJsonInt64(value));
                        }
                        catch
                        {
                            writer.Write(GetJsonDouble(value));
                        }

                        break;
                    case JsonValueKind.String: writer.Write(value.GetValue<string?>()); break;
                    case JsonValueKind.True: writer.Write(true); break;
                    case JsonValueKind.False: writer.Write(false); break;
                    default: writer.WriteNil(); break;
                }

                break;
        }
    }

    private static int GetJsonInt32(JsonNode? value)
    {
        if (value == null) return 0;
        if (value is JsonValue jv)
        {
            if (jv.TryGetValue<int>(out var i)) return i;
            if (jv.TryGetValue<long>(out var l)) return (int)l;
            if (jv.TryGetValue<double>(out var d)) return (int)d;
            if (jv.TryGetValue<float>(out var f)) return (int)f;
            if (jv.TryGetValue<short>(out var s)) return s;
            if (jv.TryGetValue<sbyte>(out var sb)) return sb;
            if (jv.TryGetValue<byte>(out var b)) return b;
        }

        return 0;
    }

    private static uint GetJsonUInt32(JsonNode? value)
    {
        if (value == null) return 0;
        if (value is JsonValue jv)
        {
            if (jv.TryGetValue<uint>(out var u)) return u;
            if (jv.TryGetValue<ulong>(out var ul)) return (uint)ul;
            if (jv.TryGetValue<int>(out var i)) return (uint)i;
            if (jv.TryGetValue<long>(out var l)) return (uint)l;
        }

        return 0;
    }

    private static long GetJsonInt64(JsonNode? value)
    {
        if (value == null) return 0;
        if (value is JsonValue jv)
        {
            if (jv.TryGetValue<long>(out var l)) return l;
            if (jv.TryGetValue<int>(out var i)) return i;
            if (jv.TryGetValue<double>(out var d)) return (long)d;
            if (jv.TryGetValue<float>(out var f)) return (long)f;
            if (jv.TryGetValue<short>(out var s)) return s;
            if (jv.TryGetValue<sbyte>(out var sb)) return sb;
        }

        return 0;
    }

    private static ulong GetJsonUInt64(JsonNode? value)
    {
        if (value == null) return 0;
        if (value is JsonValue jv)
        {
            if (jv.TryGetValue<ulong>(out var ul)) return ul;
            if (jv.TryGetValue<uint>(out var u)) return u;
            if (jv.TryGetValue<long>(out var l)) return (ulong)l;
            if (jv.TryGetValue<int>(out var i)) return (ulong)i;
        }

        return 0;
    }

    private static float GetJsonSingle(JsonNode? value)
    {
        if (value == null) return 0;
        if (value is JsonValue jv)
        {
            if (jv.TryGetValue<float>(out var f)) return f;
            if (jv.TryGetValue<double>(out var d)) return (float)d;
            if (jv.TryGetValue<int>(out var i)) return i;
            if (jv.TryGetValue<long>(out var l)) return l;
        }

        return 0;
    }

    private static double GetJsonDouble(JsonNode? value)
    {
        if (value == null) return 0;
        if (value is JsonValue jv)
        {
            if (jv.TryGetValue<double>(out var d)) return d;
            if (jv.TryGetValue<float>(out var f)) return f;
            if (jv.TryGetValue<int>(out var i)) return i;
            if (jv.TryGetValue<long>(out var l)) return l;
        }

        return 0;
    }

    private void TranscodeToNamed(ref MessagePackReader reader, ref MessagePackWriter writer, MsgPackClassSchema schema)
    {
        if (reader.TryReadNil())
        {
            writer.WriteNil();
            return;
        }

        // Union base: dispatch without re-checking nil
        if (schema.Unions.Count > 0)
        {
            TranscodeUnionToNamed(ref reader, ref writer, schema);
            return;
        }

        if (schema.IsStringKeyed)
        {
            var mapCount = reader.ReadMapHeader();
            var fieldLookup = schema.Fields.ToDictionary(f => f.StringKey ?? f.MemberName);
            writer.WriteMapHeader(mapCount);
            for (var i = 0; i < mapCount; i++)
            {
                var key = reader.ReadString()!;
                if (fieldLookup.TryGetValue(key, out var field))
                {
                    writer.Write(field.MemberName);
                    TranscodeField(ref reader, ref writer, field.TypeName, TranscodeDir.ToNamed);
                }
                else
                {
                    writer.Write(key);
                    CopyRawValue(ref reader, ref writer);
                }
            }
        }
        else
        {
            var arrCount = reader.ReadArrayHeader();
            var fieldByIndex = schema.Fields.ToDictionary(f => f.IntKey);
            writer.WriteMapHeader(arrCount);
            for (var i = 0; i < arrCount; i++)
            {
                if (fieldByIndex.TryGetValue(i, out var field))
                {
                    writer.Write(field.MemberName);
                    TranscodeField(ref reader, ref writer, field.TypeName, TranscodeDir.ToNamed);
                }
                else
                {
                    writer.Write($"_index_{i}");
                    CopyRawValue(ref reader, ref writer);
                }
            }
        }
    }

    /// <summary>Named-key msgpack map -> compact.</summary>
    private void TranscodeToCompact(ref MessagePackReader reader, ref MessagePackWriter writer,
        MsgPackClassSchema schema)
    {
        if (reader.TryReadNil())
        {
            writer.WriteNil();
            return;
        }

        // Union base: input is {"_type": discriminator, "_data": named_map}
        if (schema.Unions.Count > 0)
        {
            TranscodeUnionToCompact(ref reader, ref writer, schema);
            return;
        }

        var mapCount = reader.ReadMapHeader();
        var sequence = reader.Sequence;

        // First pass: collect MemberName -> raw value bytes
        var entries = new Dictionary<string, ReadOnlySequence<byte>>(mapCount);
        for (var i = 0; i < mapCount; i++)
        {
            var key = reader.ReadString()!;
            var valueStart = reader.Position;
            reader.Skip();
            entries[key] = sequence.Slice(valueStart, reader.Position);
        }

        if (schema.IsStringKeyed)
        {
            var fieldsPresent = schema.Fields.Where(f => entries.ContainsKey(f.MemberName)).ToList();
            writer.WriteMapHeader(fieldsPresent.Count);
            foreach (var field in fieldsPresent)
            {
                writer.Write(field.StringKey ?? field.MemberName);
                var subReader = new MessagePackReader(entries[field.MemberName]);
                TranscodeField(ref subReader, ref writer, field.TypeName, TranscodeDir.ToCompact);
            }
        }
        else
        {
            var maxKey = schema.Fields.Count > 0 ? schema.Fields.Max(f => f.IntKey) : -1;
            writer.WriteArrayHeader(maxKey + 1);
            var fieldByIndex = schema.Fields.ToDictionary(f => f.IntKey);
            for (var i = 0; i <= maxKey; i++)
            {
                if (fieldByIndex.TryGetValue(i, out var field) &&
                    entries.TryGetValue(field.MemberName, out var valueBytes))
                {
                    var subReader = new MessagePackReader(valueBytes);
                    TranscodeField(ref subReader, ref writer, field.TypeName, TranscodeDir.ToCompact);
                }
                else
                {
                    writer.WriteNil();
                }
            }
        }
    }

    /// <summary>Transcode a single field value; shared by both directions.</summary>
    private void TranscodeField(ref MessagePackReader reader, ref MessagePackWriter writer, string typeName,
        TranscodeDir dir)
    {
        if (reader.TryReadNil())
        {
            writer.WriteNil();
            return;
        }

        // Nullable<T>
        if (typeName.StartsWith("Nullable<"))
        {
            TranscodeField(ref reader, ref writer, typeName[9..^1], dir);
            return;
        }

        // Array-like types (byte[] is binary, not array)
        string? elemType = null;
        if (typeName == "byte[]")
        {
            CopyRawValue(ref reader, ref writer);
            return;
        }

        if (typeName.EndsWith("[]"))
        {
            elemType = typeName[..^2];
        }
        else if (typeName.StartsWith("List<") && typeName.EndsWith(">"))
        {
            elemType = typeName[5..^1];
        }
        else if (typeName.StartsWith("HashSet<") && typeName.EndsWith(">")) elemType = typeName[8..^1];

        if (elemType != null)
        {
            var count = reader.ReadArrayHeader();
            writer.WriteArrayHeader(count);
            for (var i = 0; i < count; i++)
            {
                TranscodeField(ref reader, ref writer, elemType, dir);
            }

            return;
        }

        // Dictionary<K,V> — copy keys verbatim (preserves original msgpack type)
        if (typeName.StartsWith("Dictionary<"))
        {
            var (_, valType) = SplitGenericArgs(typeName[11..^1]);
            var count = reader.ReadMapHeader();
            writer.WriteMapHeader(count);
            for (var i = 0; i < count; i++)
            {
                CopyRawValue(ref reader, ref writer);
                TranscodeField(ref reader, ref writer, valType, dir);
            }

            return;
        }

        // Nested schema type — recurse in the appropriate direction
        var nested = FindSchemaByTypeName(typeName);
        if (nested != null)
        {
            if (nested.Unions.Count > 0)
            {
                if (dir == TranscodeDir.ToNamed)
                {
                    TranscodeUnionToNamed(ref reader, ref writer, nested);
                }
                else
                {
                    TranscodeUnionToCompact(ref reader, ref writer, nested);
                }

                return;
            }

            if (dir == TranscodeDir.ToNamed)
            {
                TranscodeToNamed(ref reader, ref writer, nested);
            }
            else
            {
                TranscodeToCompact(ref reader, ref writer, nested);
            }

            return;
        }

        // Primitive or unknown — copy raw msgpack bytes (no type conversion)
        CopyRawValue(ref reader, ref writer);
    }

    /// <summary>Transcode union compact [discriminator, payload] -> {"_type":d,"_data":{named map}}.</summary>
    private void TranscodeUnionToNamed(ref MessagePackReader reader, ref MessagePackWriter writer,
        MsgPackClassSchema unionSchema)
    {
        reader.ReadArrayHeader(); // 2
        var discriminator = reader.ReadInt32();
        var entry = unionSchema.Unions.FirstOrDefault(u => u.Key == discriminator);
        var concreteSchema = entry != null ? FindSchemaByTypeName(entry.TypeName) : null;
        writer.WriteMapHeader(2);
        writer.Write("_type");
        writer.Write(discriminator);
        writer.Write("_data");
        if (concreteSchema != null)
        {
            TranscodeToNamed(ref reader, ref writer, concreteSchema);
        }
        else
        {
            CopyRawValue(ref reader, ref writer);
        }
    }

    /// <summary>Transcode union named {"_type":d,"_data":{named map}} -> compact [discriminator, payload].</summary>
    private void TranscodeUnionToCompact(ref MessagePackReader reader, ref MessagePackWriter writer,
        MsgPackClassSchema unionSchema)
    {
        var mapCount = reader.ReadMapHeader();
        var sequence = reader.Sequence;
        var entries = new Dictionary<string, ReadOnlySequence<byte>>(mapCount);
        for (var i = 0; i < mapCount; i++)
        {
            var key = reader.ReadString()!;
            var valueStart = reader.Position;
            reader.Skip();
            entries[key] = sequence.Slice(valueStart, reader.Position);
        }

        var discriminator = 0;
        if (entries.TryGetValue("_type", out var typeBytes))
        {
            var tr = new MessagePackReader(typeBytes);
            discriminator = tr.ReadInt32();
        }

        var entry = unionSchema.Unions.FirstOrDefault(u => u.Key == discriminator);
        var concreteSchema = entry != null ? FindSchemaByTypeName(entry.TypeName) : null;
        writer.WriteArrayHeader(2);
        writer.Write(discriminator);
        if (concreteSchema != null && entries.TryGetValue("_data", out var dataBytes))
        {
            var dr = new MessagePackReader(dataBytes);
            TranscodeToCompact(ref dr, ref writer, concreteSchema);
        }
        else
        {
            writer.WriteNil();
        }
    }

    /// <summary>Copy one complete msgpack value from reader to writer without interpretation.</summary>
    private static void CopyRawValue(ref MessagePackReader reader, ref MessagePackWriter writer)
    {
        var start = reader.Position;
        reader.Skip();
        writer.WriteRaw(reader.Sequence.Slice(start, reader.Position));
    }

    private MsgPackClassSchema FindSchema(string name)
    {
        if (_schemas.TryGetValue(name, out var s)) return s;
        var match = _schemas.Values.FirstOrDefault(x =>
            x.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
            x.FullName.EndsWith("." + name, StringComparison.OrdinalIgnoreCase));
        return match ?? throw new ArgumentException($"Schema not found: {name}");
    }

    private MsgPackClassSchema? FindSchemaByTypeName(string typeName)
    {
        // Try direct lookup
        if (_schemas.TryGetValue(typeName, out var s)) return s;
        // Try by simple name
        return _schemas.Values.FirstOrDefault(x =>
            x.Name == typeName ||
            x.FullName.EndsWith("." + typeName));
    }

    private static (string, string) SplitGenericArgs(string args)
    {
        var depth = 0;
        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case '<':
                    depth++;
                    break;
                case '>':
                    depth--;
                    break;
                case ',' when depth == 0:
                    return (args[..i].Trim(), args[(i + 1)..].Trim());
            }
        }

        return (args, "string");
    }

    private enum TranscodeDir
    {
        ToNamed,
        ToCompact
    }
}

/// <summary>
///     Simple growable byte buffer for MessagePackWriter.
/// </summary>
internal class ArrayBufferWriter(int initialCapacity = 65536) : IBufferWriter<byte>
{
    private byte[] _buffer = new byte[initialCapacity];
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