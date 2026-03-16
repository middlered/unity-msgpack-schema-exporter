using System.Buffers;
using System.Text.Json;
using System.Text.Json.Nodes;
using MessagePack;
using UnityMsgpackSchemaExporter;

// CLI entry

if (args.Length == 0)
{
    PrintUsage();
    return 0;
}

var command = args[0].ToLowerInvariant();

if (command is "help" or "-h" or "--help")
{
    PrintUsage();
    return 0;
}

// Default DummyDll path
var dummyDllPath = Environment.GetEnvironmentVariable("MSGPACK_ASSEMBLY_PATH")
                   ?? FindDummyDllPath()
                   ?? "";

// Parse --dll option
for (var i = 0; i < args.Length - 1; i++)
{
    if (args[i] == "--dll")
    {
        dummyDllPath = args[i + 1];
        // Remove these args
        args = args.Take(i).Concat(args.Skip(i + 2)).ToArray();
        break;
    }
}

if (string.IsNullOrEmpty(dummyDllPath) && command is not "raw-decode")
{
    Console.Error.WriteLine("Error: DummyDll path not found. Use --dll <path> or set MSGPACK_ASSEMBLY_PATH env var.");
    return 1;
}

try
{
    return command switch
    {
        "list" => CmdList(dummyDllPath, args.Skip(1).ToArray()),
        "schema" => CmdSchema(dummyDllPath, args.Skip(1).ToArray()),
        "avro" => CmdAvro(dummyDllPath, args.Skip(1).ToArray()),
        "decode" => CmdDecode(dummyDllPath, args.Skip(1).ToArray()),
        "encode" => CmdEncode(dummyDllPath, args.Skip(1).ToArray()),
        "to-named" => CmdToNamed(dummyDllPath, args.Skip(1).ToArray()),
        "to-compact" => CmdToCompact(dummyDllPath, args.Skip(1).ToArray()),
        "raw-decode" => CmdRawDecode(args.Skip(1).ToArray()),
        "stats" => CmdStats(dummyDllPath, args.Skip(1).ToArray()),
        "diff" => CmdDiff(args.Skip(1).ToArray()),
        _ => Error($"Unknown command: {command}")
    };
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Error: {ex.Message}");
    return 1;
}

// ============================================================================
// Commands
// ============================================================================

int CmdList(string dllPath, string[] cmdArgs)
{
    using var extractor = LoadSchemas(dllPath);
    var schemas = extractor.Schemas;

    var filter = cmdArgs.FirstOrDefault(a => !a.StartsWith('-'));
    var list = filter != null
        ? extractor.SearchSchemas(filter).ToList()
        : schemas.Values.ToList();

    list.Sort((a, b) => string.Compare(a.FullName, b.FullName, StringComparison.Ordinal));

    var verbose = cmdArgs.Contains("-v");

    Console.WriteLine($"Total: {list.Count} MessagePackObject classes");
    Console.WriteLine(new string('─', 80));

    foreach (var s in list)
    {
        if (verbose)
        {
            var keyType = s.IsStringKeyed ? "str" : "int";
            Console.WriteLine($"  [{keyType}] {s.FullName} ({s.Fields.Count} fields) [{s.SourceAssembly}]");
        }
        else
        {
            Console.WriteLine($"  {s.FullName}");
        }
    }

    return 0;
}

int CmdSchema(string dllPath, string[] cmdArgs)
{
    if (cmdArgs.Length == 0)
    {
        return Error("Usage: schema <className> [--json]");
    }

    using var extractor = LoadSchemas(dllPath);
    var name = cmdArgs.First(a => !a.StartsWith("--"));
    var jsonMode = cmdArgs.Contains("--json");
    var schema = extractor.FindSchema(name);

    if (schema == null)
    {
        // Try search
        var matches = extractor.SearchSchemas(name).Take(20).ToList();
        if (matches.Count > 0)
        {
            Console.Error.WriteLine($"Schema '{name}' not found. Did you mean:");
            foreach (var m in matches)
            {
                Console.Error.WriteLine($"  {m.FullName}");
            }
        }
        else
        {
            Console.Error.WriteLine($"Schema '{name}' not found.");
        }

        return 1;
    }

    if (jsonMode)
    {
        var obj = new JsonObject
        {
            ["fullName"] = schema.FullName,
            ["assemblyName"] = schema.SourceAssembly,
            ["keyType"] = schema.IsStringKeyed ? "string" : "int",
            ["fields"] = new JsonArray(schema.Fields.Select(f => (JsonNode)new JsonObject
            {
                ["key"] = schema.IsStringKeyed ? f.StringKey ?? f.MemberName : f.IntKey,
                ["memberName"] = f.MemberName,
                ["typeName"] = f.TypeName,
                ["isProperty"] = f.IsProperty
            }).ToArray())
        };
        if (schema.Unions.Count > 0)
        {
            obj["unions"] = new JsonArray(schema.Unions.Select(u => (JsonNode)new JsonObject
            {
                ["key"] = u.Key,
                ["typeName"] = u.TypeName
            }).ToArray());
        }

        Console.WriteLine(obj.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
    }
    else
    {
        PrintSchema(schema);
    }

    return 0;
}

int CmdAvro(string dllPath, string[] cmdArgs)
{
    if (cmdArgs.Length == 0)
    {
        return Error("Usage: avro <className> [--all] [--out <dir>] [--nullable] [--flat]");
    }

    using var extractor = LoadSchemas(dllPath);
    var schemas = extractor.Schemas;

    var all = cmdArgs.Contains("--all");
    var nullable = cmdArgs.Contains("--nullable");
    var flat = cmdArgs.Contains("--flat");
    string? outDir = null;
    for (var i = 0; i < cmdArgs.Length - 1; i++)
    {
        if (cmdArgs[i] == "--out")
        {
            outDir = cmdArgs[i + 1];
        }
    }

    var options = new AvroExportOptions { AllNullable = nullable, Flat = flat };

    if (all)
    {
        outDir ??= "avro_schemas";
        Directory.CreateDirectory(outDir);

        if (flat)
        {
            // Flat mode: export all schemas into a single protocol file
            var allAvro = AvroExporter.ExportAllSchemas(schemas, options);
            var protocolPath = Path.Combine(outDir, "_all_schemas.avsc");
            File.WriteAllText(protocolPath, allAvro.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            Console.WriteLine($"Exported {allAvro.Count} Avro schemas (flat) to {protocolPath}");
        }
        else
        {
            foreach (var s in schemas.Values)
            {
                var avro = AvroExporter.ExportSchema(s, schemas, options);
                var path = Path.Combine(outDir, s.FullName.Replace('.', '_') + ".avsc");
                File.WriteAllText(path, avro.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            }

            Console.WriteLine($"Exported {schemas.Count} Avro schemas to {outDir}/");
        }
    }
    else
    {
        var name = cmdArgs.First(a => !a.StartsWith("--"));
        var schema = extractor.FindSchema(name);
        if (schema == null)
        {
            return Error($"Schema '{name}' not found.");
        }

        var avro = AvroExporter.ExportSchema(schema, schemas, options);
        Console.WriteLine(avro.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
    }

    return 0;
}

int CmdDecode(string dllPath, string[] cmdArgs)
{
    if (cmdArgs.Length < 2)
    {
        return Error("Usage: decode <className> <hex|base64|@file>");
    }

    using var extractor = LoadSchemas(dllPath);
    var codec = new MsgPackCodec(extractor.Schemas);

    var className = cmdArgs[0];
    var input = cmdArgs[1];
    var data = ReadBinaryInput(input);

    var json = codec.DecodeJson(className, data);
    Console.WriteLine(json!.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
    return 0;
}

int CmdEncode(string dllPath, string[] cmdArgs)
{
    if (cmdArgs.Length < 2)
    {
        return Error("Usage: encode <className> <json|@file> [--hex|--base64|--out <file>]");
    }

    using var extractor = LoadSchemas(dllPath);
    var codec = new MsgPackCodec(extractor.Schemas);

    var className = cmdArgs[0];
    var input = cmdArgs[1];
    var jsonStr = input.StartsWith("@")
        ? File.ReadAllText(input[1..])
        : input;

    var json = JsonNode.Parse(jsonStr);
    var data = codec.EncodeJson(className, json);

    var hex = cmdArgs.Contains("--hex");
    string? outFile = null;
    for (var i = 0; i < cmdArgs.Length - 1; i++)
    {
        if (cmdArgs[i] == "--out")
        {
            outFile = cmdArgs[i + 1];
        }
    }

    if (outFile != null)
    {
        File.WriteAllBytes(outFile, data);
        Console.Error.WriteLine($"Written {data.Length} bytes to {outFile}");
    }
    else if (hex)
    {
        Console.WriteLine(Convert.ToHexString(data).ToLowerInvariant());
    }
    else
    {
        Console.WriteLine(Convert.ToBase64String(data));
    }

    return 0;
}

int CmdRawDecode(string[] cmdArgs)
{
    if (cmdArgs.Length == 0)
    {
        return Error("Usage: raw-decode <hex|base64|@file>");
    }

    var data = ReadBinaryInput(cmdArgs[0]);
    var reader = new MessagePackReader(data);
    var json = ReadAnyRecursive(ref reader);
    Console.WriteLine(json!.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
    return 0;
}

int CmdToNamed(string dllPath, string[] cmdArgs)
{
    if (cmdArgs.Length < 2)
    {
        return Error("Usage: to-named <className> <hex|base64|@file> [--hex|--base64|--out <file>]");
    }

    using var extractor = LoadSchemas(dllPath);
    var codec = new MsgPackCodec(extractor.Schemas);

    var className = cmdArgs[0];
    var input = cmdArgs[1];
    var data = ReadBinaryInput(input);

    var named = codec.Decode(className, data);
    WriteBinaryOutput(named, cmdArgs);
    return 0;
}

int CmdToCompact(string dllPath, string[] cmdArgs)
{
    if (cmdArgs.Length < 2)
    {
        return Error("Usage: to-compact <className> <hex|base64|@file> [--hex|--base64|--out <file>]");
    }

    using var extractor = LoadSchemas(dllPath);
    var codec = new MsgPackCodec(extractor.Schemas);

    var className = cmdArgs[0];
    var input = cmdArgs[1];
    var data = ReadBinaryInput(input);

    var compact = codec.Encode(className, data);
    WriteBinaryOutput(compact, cmdArgs);
    return 0;
}

int CmdStats(string dllPath, string[] cmdArgs)
{
    using var extractor = LoadSchemas(dllPath);
    var schemas = extractor.Schemas;

    var stringKeyed = schemas.Values.Count(s => s.IsStringKeyed);
    var intKeyed = schemas.Values.Count(s => !s.IsStringKeyed);
    var totalFields = schemas.Values.Sum(s => s.Fields.Count);
    var withUnion = schemas.Values.Count(s => s.Unions.Count > 0);
    var abstractCount = schemas.Values.Count(s => s.IsAbstract);

    var assemblies = schemas.Values.Select(s => s.SourceAssembly).Distinct().ToList();
    var namespaces = schemas.Values.Select(s => s.Namespace).Distinct().OrderBy(x => x).ToList();

    Console.WriteLine("═══ MessagePack Schema Statistics ═══");
    Console.WriteLine($"  Total classes:    {schemas.Count}");
    Console.WriteLine($"  String-keyed:     {stringKeyed}");
    Console.WriteLine($"  Integer-keyed:    {intKeyed}");
    Console.WriteLine($"  Abstract:         {abstractCount}");
    Console.WriteLine($"  With Union:       {withUnion}");
    Console.WriteLine($"  Total fields:     {totalFields}");
    Console.WriteLine($"  Assemblies:       {assemblies.Count}");
    Console.WriteLine($"  Namespaces:       {namespaces.Count}");
    Console.WriteLine();
    Console.WriteLine("Namespaces:");
    foreach (var ns in namespaces)
    {
        Console.WriteLine(
            $"  {(string.IsNullOrEmpty(ns) ? "(global)" : ns)}: {schemas.Values.Count(s => s.Namespace == ns)} classes");
    }

    return 0;
}

int CmdDiff(string[] cmdArgs)
{
    if (cmdArgs.Length < 2)
    {
        return Error("Usage: diff <dummyDllPath1> <dummyDllPath2>");
    }

    using var ext1 = LoadSchemas(cmdArgs[0]);
    using var ext2 = LoadSchemas(cmdArgs[1]);

    var schemas1 = ext1.Schemas;
    var schemas2 = ext2.Schemas;

    var added = schemas2.Keys.Except(schemas1.Keys).OrderBy(x => x).ToList();
    var removed = schemas1.Keys.Except(schemas2.Keys).OrderBy(x => x).ToList();
    var common = schemas1.Keys.Intersect(schemas2.Keys).OrderBy(x => x).ToList();

    var modified = 0;
    var changes = new List<string>();

    foreach (var name in common)
    {
        var s1 = schemas1[name];
        var s2 = schemas2[name];
        var diffs = CompareSchemas(s1, s2);
        if (diffs.Count > 0)
        {
            modified++;
            changes.Add($"\n  ≠ {name}:");
            foreach (var d in diffs)
            {
                changes.Add($"      {d}");
            }
        }
    }

    Console.WriteLine("═══ Schema Diff ═══");
    Console.WriteLine($"  Path A: {cmdArgs[0]}");
    Console.WriteLine($"  Path B: {cmdArgs[1]}");
    Console.WriteLine($"  Classes in A: {schemas1.Count}");
    Console.WriteLine($"  Classes in B: {schemas2.Count}");
    Console.WriteLine($"  Added:        {added.Count}");
    Console.WriteLine($"  Removed:      {removed.Count}");
    Console.WriteLine($"  Modified:     {modified}");
    Console.WriteLine($"  Unchanged:    {common.Count - modified}");

    if (added.Count > 0)
    {
        Console.WriteLine($"\n── Added ({added.Count}) ──");
        foreach (var a in added) Console.WriteLine($"  + {a}");
    }

    if (removed.Count > 0)
    {
        Console.WriteLine($"\n── Removed ({removed.Count}) ──");
        foreach (var r in removed) Console.WriteLine($"  - {r}");
    }

    if (changes.Count > 0)
    {
        Console.WriteLine($"\n── Modified ({modified}) ──");
        foreach (var c in changes) Console.WriteLine(c);
    }

    return 0;
}

// ============================================================================
// Helpers
// ============================================================================

SchemaExtractor LoadSchemas(string dllPath)
{
    var extractor = new SchemaExtractor();
    if (Directory.Exists(dllPath))
    {
        extractor.LoadDirectory(dllPath);
    }
    else if (File.Exists(dllPath))
    {
        extractor.LoadAssembly(dllPath);
    }
    else
    {
        throw new FileNotFoundException($"DummyDll path not found: {dllPath}");
    }

    extractor.Extract();
    return extractor;
}

void PrintSchema(MsgPackClassSchema schema)
{
    var rows = schema.Fields.Select(f => new
    {
        Key = f.IsStringKeyed ? $"\"{f.StringKey}\"" : $"[{f.IntKey}]",
        Type = f.TypeName,
        Member = f.MemberName
    }).ToList();

    // use the max length in these string as the column width
    var keyWidth = Math.Max(rows.Any() ? rows.Max(r => r.Key.Length) : 0, 10);
    var typeWidth = Math.Max(rows.Any() ? rows.Max(r => r.Type.Length) : 0, 5);
    var memberWidth = Math.Max(rows.Any() ? rows.Max(r => r.Member.Length) : 0, 10);

    var totalWidth = keyWidth + typeWidth + memberWidth + 4;
    var thinLine = new string('-', totalWidth);

    // area: header
    Console.WriteLine(thinLine);
    Console.WriteLine($"Class   : {schema.FullName}");
    Console.WriteLine($"Assembly: {schema.SourceAssembly}");
    Console.WriteLine($"Format  : {(schema.IsStringKeyed ? "String (Map)" : "Integer (Array)")}");

    if (schema.BaseTypeName != null && schema.BaseTypeName != "System.Object")
    {
        Console.WriteLine($"Base:      {schema.BaseTypeName}");
    }

    if (schema.IsAbstract)
    {
        Console.WriteLine("Modifier:  Abstract");
    }

    if (schema.Unions.Count > 0)
    {
        Console.WriteLine("Unions:    " + string.Join(", ", schema.Unions.Select(u => $"[{u.Key}]->{u.TypeName}")));
    }

    // area: field
    Console.WriteLine(thinLine);
    Console.WriteLine($"{"Key".PadRight(keyWidth)}  {"Type".PadRight(typeWidth)}  {"Member".PadRight(memberWidth)}");
    Console.WriteLine(thinLine);

    foreach (var r in rows)
    {
        Console.WriteLine(
            $"{r.Key.PadRight(keyWidth)}  {r.Type.PadRight(typeWidth)}  {r.Member.PadRight(memberWidth)}");
    }

    Console.WriteLine(thinLine);
}

byte[] ReadBinaryInput(string input)
{
    if (input.StartsWith("@")) return File.ReadAllBytes(input[1..]);

    // Try hex
    input = input.Trim();
    if (IsHex(input)) return Convert.FromHexString(input);

    // Try base64
    try
    {
        return Convert.FromBase64String(input);
    }
    catch
    {
        throw new ArgumentException(
            $"Cannot parse input as hex, base64, or file path (prefix with @): {input[..Math.Min(40, input.Length)]}...");
    }
}

bool IsHex(string s)
{
    return s.Length >= 2 && s.Length % 2 == 0 && s.All(c => "0123456789abcdefABCDEF".Contains(c));
}

void WriteBinaryOutput(byte[] data, string[] cmdArgs)
{
    var hex = cmdArgs.Contains("--hex");
    var base64 = cmdArgs.Contains("--base64");
    string? outFile = null;
    for (var i = 0; i < cmdArgs.Length - 1; i++)
    {
        if (cmdArgs[i] == "--out") outFile = cmdArgs[i + 1];
    }

    if (outFile != null)
    {
        File.WriteAllBytes(outFile, data);
        Console.Error.WriteLine($"Written {data.Length} bytes to {outFile}");
    }
    else if (hex)
    {
        Console.WriteLine(Convert.ToHexString(data).ToLowerInvariant());
    }
    else if (base64)
    {
        Console.WriteLine(Convert.ToBase64String(data));
    }
    else
    {
        // Default to hex for binary output
        Console.WriteLine(Convert.ToHexString(data).ToLowerInvariant());
    }
}

JsonNode? ReadAnyRecursive(ref MessagePackReader reader)
{
    var type = reader.NextMessagePackType;

    switch (type)
    {
        case MessagePackType.Nil:
            reader.ReadNil();
            return null;
        case MessagePackType.Boolean:
            return JsonValue.Create(reader.ReadBoolean());
        case MessagePackType.Integer:
            try
            {
                return JsonValue.Create(reader.ReadInt64());
            }
            catch
            {
                return JsonValue.Create(reader.ReadUInt64());
            }

        case MessagePackType.Float:
            return JsonValue.Create(reader.ReadDouble());
        case MessagePackType.String:
            return JsonValue.Create(reader.ReadString());
        case MessagePackType.Binary:
            var binSeq = reader.ReadBytes();
            if (!binSeq.HasValue) return null;

            var buf = new byte[binSeq.Value.Length];
            binSeq.Value.CopyTo(buf);
            return JsonValue.Create(Convert.ToBase64String(buf));

        case MessagePackType.Array:
            var arrLen = reader.ReadArrayHeader();
            var arr = new JsonArray();
            for (var i = 0; i < arrLen; i++)
            {
                arr.Add(ReadAnyRecursive(ref reader));
            }

            return arr;

        case MessagePackType.Map:
            var mapLen = reader.ReadMapHeader();
            var obj = new JsonObject();
            for (var i = 0; i < mapLen; i++)
            {
                var key = ReadAnyRecursive(ref reader)?.ToJsonString() ?? "null";
                // Remove quotes from string keys
                if (key.StartsWith('"') && key.EndsWith('"'))
                {
                    key = key[1..^1];
                }

                var val = ReadAnyRecursive(ref reader);
                obj[key] = val;
            }

            return obj;

        default:
            reader.Skip();
            return null;
    }
}

List<string> CompareSchemas(MsgPackClassSchema a, MsgPackClassSchema b)
{
    var diffs = new List<string>();

    var aFields = a.Fields.ToDictionary(f => f.EffectiveKey);
    var bFields = b.Fields.ToDictionary(f => f.EffectiveKey);

    foreach (var key in bFields.Keys.Except(aFields.Keys))
    {
        diffs.Add($"+ field {key}: {bFields[key].TypeName} {bFields[key].MemberName}");
    }

    foreach (var key in aFields.Keys.Except(bFields.Keys))
    {
        diffs.Add($"- field {key}: {aFields[key].TypeName} {aFields[key].MemberName}");
    }

    foreach (var key in aFields.Keys.Intersect(bFields.Keys))
    {
        if (aFields[key].TypeName != bFields[key].TypeName)
        {
            diffs.Add($"~ field {key}: type {aFields[key].TypeName} -> {bFields[key].TypeName}");
        }
    }

    return diffs;
}

string? FindDummyDllPath()
{
    var candidates = new[]
    {
        "DummyDll",
        "../DummyDll",
        Path.Combine(Directory.GetCurrentDirectory(), "DummyDll")
    };
    return candidates.FirstOrDefault(Directory.Exists);
}

int Error(string msg)
{
    Console.Error.WriteLine(msg);
    return 1;
}


void PrintUsage()
{
    Console.WriteLine(
        """
        MessagePack Schema Tools for Unity dummy dll (or any other .net assembly)         
        Usage: unityMsgpackSchemaExporter <command> [options]

        Commands:
          list [filter] [-v]              List all MessagePackObject classes
          schema <className>              Show schema definition for a class
          
          avro <className>                Export Avro schema JSON (nested mode)
          avro --all [--out <dir>]        Export all schemas as Avro
          avro <className> --flat         Export Avro with types referenced by name
          avro <className> --nullable     Wrap all fields as nullable
          avro --all --flat --out <dir>   Flat export all into single file
          
          decode <className> <data>       Decode compact msgpack -> JSON with keys
          encode <className> <json>       Encode JSON -> compact msgpack binary
          to-named <className> <data>     Decode compact msgpack -> named-key msgpack
          to-compact <className> <data>   Encode named-key msgpack -> compact msgpack
          raw-decode <data>               Decode raw msgpack without schema
          
          stats                           Show statistics about loaded schemas
          diff <dllPath1> <dllPath2>      Compare schemas between two versions

        Options:
          --dll <path>                    Path to DummyDll directory or a single .dll file
                                          If a directory is given, all assemblies inside are loaded.
                                          If a single .dll file is given, only that file is loaded.
          --hex                           Output encode result as hex
          --out <file>                    Write binary output to file

        Data input formats:
          hex string                      e.g., 92c0a568656c6c6f
          base64 string                   e.g., ksBlaGVsbG8=
          @filename                       Read from file

        Environment:
          UNITY_MSGPACK_PATH             Default path to DummyDll directory or a single .dll file

        Examples:
          unityMsgpackSchemaExporter list User
          unityMsgpackSchemaExporter list --dll ./DummyDll 
          unityMsgpackSchemaExporter schema MyMsgPack --dll ./Assembly-CSharp.dll
          unityMsgpackSchemaExporter avro AssetBundleElement
          unityMsgpackSchemaExporter avro --all --out ./schemas/
          unityMsgpackSchemaExporter avro MyMsgPack --flat --nullable
          unityMsgpackSchemaExporter decode MyMsgPack 84a26964c8a76772...
          unityMsgpackSchemaExporter encode MyMsgPack '{"id":1,"description":"test"}'
          unityMsgpackSchemaExporter diff DummyDll/ ../other_version/DummyDll/
        """
    );
}