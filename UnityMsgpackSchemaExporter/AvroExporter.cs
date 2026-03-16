using System.Text.Json;
using System.Text.Json.Nodes;

namespace UnityMsgpackSchemaExporter;

/// <summary>
///     Options controlling Avro schema export behavior.
/// </summary>
public class AvroExportOptions
{
    /// <summary>When true, wrap every field type in ["null", type] (union with null).</summary>
    public bool AllNullable { get; set; }

    /// <summary>
    ///     When true, export in flat mode: nested MessagePack types are referenced by name
    ///     and exported as separate top-level records in ExportAllSchemas.
    ///     When false (default), nested types are inlined recursively.
    /// </summary>
    public bool Flat { get; set; }

    public static AvroExportOptions Default => new();
}

/// <summary>
///     Exports MsgPackClassSchema as Apache Avro schema JSON.
///     Supports nested (inline) and flat (reference) modes, and optional all-nullable wrapping.
/// </summary>
public static class AvroExporter
{
    public static JsonObject ExportSchema(
        MsgPackClassSchema schema,
        IReadOnlyDictionary<string, MsgPackClassSchema> allSchemas,
        AvroExportOptions? options = null)
    {
        options ??= AvroExportOptions.Default;
        var emitted = new HashSet<string>();
        return ExportSchemaInternal(schema, allSchemas, emitted, options);
    }

    /// <summary>
    ///     Export all schemas. In flat mode, returns one record per schema with cross-references.
    ///     In nested mode (default), each schema inlines its dependencies.
    /// </summary>
    public static JsonArray ExportAllSchemas(
        IReadOnlyDictionary<string, MsgPackClassSchema> allSchemas,
        AvroExportOptions? options = null)
    {
        options ??= AvroExportOptions.Default;
        var result = new JsonArray();

        if (options.Flat)
            // Flat mode: export each schema once, nested types referenced by qualified name
        {
            foreach (var schema in allSchemas.Values.OrderBy(s => s.FullName))
            {
                var emitted = new HashSet<string>();
                result.Add(ExportSchemaInternal(schema, allSchemas, emitted, options));
            }
        }
        else
            // Nested mode: each top-level schema gets its own emitted set
        {
            foreach (var schema in allSchemas.Values.OrderBy(s => s.FullName))
            {
                var emitted = new HashSet<string>();
                result.Add(ExportSchemaInternal(schema, allSchemas, emitted, options));
            }
        }

        return result;
    }

    private static JsonObject ExportSchemaInternal(
        MsgPackClassSchema schema,
        IReadOnlyDictionary<string, MsgPackClassSchema> allSchemas,
        HashSet<string> emitted,
        AvroExportOptions options)
    {
        emitted.Add(schema.FullName);

        var record = new JsonObject
        {
            ["type"] = "record",
            ["name"] = schema.Name,
            ["namespace"] = string.IsNullOrEmpty(schema.Namespace) ? "msgpack" : schema.Namespace
        };

        var fields = new JsonArray();
        foreach (var f in schema.Fields)
        {
            var avroType = ConvertTypeToAvro(f.TypeName, allSchemas, emitted, options);

            // --nullable: wrap in ["null", type] if not already nullable
            if (options.AllNullable)
            {
                avroType = WrapNullable(avroType);
            }

            var field = new JsonObject
            {
                ["name"] = f.StringKey ?? f.MemberName,
                ["type"] = avroType
            };

            if (f.IsStringKeyed)
            {
                field["msgpack_key"] = f.StringKey;
            }
            else
            {
                field["msgpack_key"] = f.IntKey;
            }

            fields.Add((JsonNode?)field);
        }

        record["fields"] = fields;

        if (schema.Unions.Count > 0)
        {
            var unions = new JsonArray();
            foreach (var u in schema.Unions)
            {
                unions.Add((JsonNode?)new JsonObject { ["key"] = u.Key, ["type"] = u.TypeName });
            }

            record["msgpack_unions"] = unions;
        }

        return record;
    }

    /// <summary>Wrap a type in ["null", type] if it isn't already nullable.</summary>
    private static JsonNode? WrapNullable(JsonNode? avroType)
    {
        // Already a union array containing "null"
        if (avroType is JsonArray arr &&
            arr.Any(t => t?.GetValueKind() == JsonValueKind.String && t.GetValue<string>() == "null"))
        {
            return avroType;
        }

        // Already "null" itself
        if (avroType?.GetValueKind() == JsonValueKind.String && avroType.GetValue<string>() == "null")
        {
            return avroType;
        }

        var result = new JsonArray();
        result.Add((JsonNode?)"null");
        result.Add(avroType?.DeepClone());
        return result;
    }

    private static JsonNode? ConvertTypeToAvro(
        string typeName,
        IReadOnlyDictionary<string, MsgPackClassSchema> allSchemas,
        HashSet<string> emitted,
        AvroExportOptions options)
    {
        // Handle nullable
        if (typeName.StartsWith("Nullable<") && typeName.EndsWith(">"))
        {
            var inner = typeName[9..^1];
            var result = new JsonArray();
            result.Add((JsonNode?)"null");
            result.Add(ConvertTypeToAvro(inner, allSchemas, emitted, options));
            return result;
        }

        // Handle byte[] specially — Avro "bytes" type
        if (typeName == "byte[]")
        {
            return "bytes";
        }

        // Handle arrays
        if (typeName.EndsWith("[]"))
        {
            var elem = typeName[..^2];
            return new JsonObject
            {
                ["type"] = "array",
                ["items"] = ConvertTypeToAvro(elem, allSchemas, emitted, options)
            };
        }

        // Handle List<T>
        if (typeName.StartsWith("List<") && typeName.EndsWith(">"))
        {
            var inner = typeName[5..^1];
            return new JsonObject
            {
                ["type"] = "array",
                ["items"] = ConvertTypeToAvro(inner, allSchemas, emitted, options)
            };
        }

        // Handle HashSet<T>
        if (typeName.StartsWith("HashSet<") && typeName.EndsWith(">"))
        {
            var inner = typeName[8..^1];
            return new JsonObject
            {
                ["type"] = "array",
                ["items"] = ConvertTypeToAvro(inner, allSchemas, emitted, options)
            };
        }

        // Handle Dictionary<K,V>
        if (typeName.StartsWith("Dictionary<"))
        {
            var inner = typeName[11..^1];
            var (keyType, valType) = SplitGenericArgs(inner);
            if (keyType == "string")
            {
                return new JsonObject
                {
                    ["type"] = "map",
                    ["values"] = ConvertTypeToAvro(valType, allSchemas, emitted, options)
                };
            }

            // Non-string key: Avro maps require string keys.  We still emit an Avro
            // map (keys are stringified at the Avro layer) and annotate the original
            // msgpack key type via the custom "msgpack_key_type" field so downstream
            // tooling can reconstruct the correct type.
            return new JsonObject
            {
                ["type"] = "map",
                ["values"] = ConvertTypeToAvro(valType, allSchemas, emitted, options),
                ["msgpack_key_type"] = keyType
            };
        }

        // Primitive types
        switch (typeName)
        {
            case "int" or "short" or "ushort" or "sbyte" or "byte":
                return "int";
            case "long" or "ulong" or "uint":
                return "long";
            case "float":
                return "float";
            case "double":
                return "double";
            case "bool":
                return "boolean";
            case "string":
                return new JsonArray("null", "string");
            case "byte[]":
                return "bytes";
            case "object":
                return new JsonArray("null", "string", "int", "long", "float", "double", "boolean");
        }

        // Check if this is a known MessagePack nested type
        var nestedSchema = FindSchemaByTypeName(typeName, allSchemas);

        // Unknown / enum type — fall back to string
        if (nestedSchema == null) return "string";

        // Build qualified name for reference
        var qualifiedName = string.IsNullOrEmpty(nestedSchema.Namespace)
            ? nestedSchema.Name
            : $"{nestedSchema.Namespace}.{nestedSchema.Name}";

        // Flat mode: always reference by qualified name
        if (options.Flat) return qualifiedName;

        // Already emitted — reference by qualified name (avoid infinite recursion)
        if (emitted.Contains(nestedSchema.FullName)) return qualifiedName;

        // Recursively inline the nested record
        return ExportSchemaInternal(nestedSchema, allSchemas, emitted, options);
    }

    /// <summary>
    ///     Find schema by simple name or full name (matches how type names appear in Cecil).
    /// </summary>
    private static MsgPackClassSchema? FindSchemaByTypeName(
        string typeName, IReadOnlyDictionary<string, MsgPackClassSchema> allSchemas)
    {
        if (allSchemas.TryGetValue(typeName, out var s)) return s;
        return allSchemas.Values.FirstOrDefault(x =>
            x.Name == typeName || x.FullName.EndsWith("." + typeName));
    }

    private static (string, string) SplitGenericArgs(string args)
    {
        var depth = 0;
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] == '<')
            {
                depth++;
            }
            else if (args[i] == '>')
            {
                depth--;
            }
            else if (args[i] == ',' && depth == 0) return (args[..i].Trim(), args[(i + 1)..].Trim());
        }

        return (args, "string");
    }

    private static string SanitizeName(string s)
    {
        return s.Replace("<", "_").Replace(">", "_").Replace(",", "_").Replace(" ", "").Replace("[]", "Arr");
    }
}