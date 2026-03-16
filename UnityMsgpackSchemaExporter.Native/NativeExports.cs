using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json.Nodes;

namespace UnityMsgpackSchemaExporter.Native;

/// <summary>
///     Native C-ABI exports for MsgPack Schema Tools.
///     Call from Python/Go/Rust/C via FFI (ctypes, cgo, etc.)
///     Memory convention:
///     - Returned strings must be freed with msgpack_free_string().
///     - Returned byte arrays must be freed with msgpack_free_bytes().
///     - Input strings are not freed (caller manages them).
/// </summary>
public static class NativeExports
{
    private static SchemaExtractor? _extractor;
    private static MsgPackCodec? _codec;
    private static string? _lastError;

    /// <summary>
    ///     Initialize the library with a DummyDll directory path.
    ///     Returns 0 on success, -1 on error.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_init")]
    public static int Init(IntPtr dummyDllPathPtr)
    {
        try
        {
            var path = Marshal.PtrToStringUTF8(dummyDllPathPtr)!;
            _extractor?.Dispose();
            _extractor = new SchemaExtractor();
            if (Directory.Exists(path))
            {
                _extractor.LoadDirectory(path);
            }
            else if (File.Exists(path))
            {
                _extractor.LoadAssembly(path);
            }
            else
            {
                _lastError = $"Path not found: {path}";
                return -1;
            }

            _extractor.Extract();
            _codec = new MsgPackCodec(_extractor.Schemas);
            _lastError = null;
            return 0;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return -1;
        }
    }

    /// <summary>Get last error message. Returned string must be freed with msgpack_free_string.</summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_get_error")]
    public static IntPtr GetError()
    {
        return AllocString(_lastError ?? "");
    }

    /// <summary>Get total schema count.</summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_schema_count")]
    public static int SchemaCount()
    {
        return _extractor?.Schemas.Count ?? 0;
    }

    /// <summary>
    ///     List all class names as JSON array. Returned string must be freed.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_list_classes")]
    public static IntPtr ListClasses()
    {
        try
        {
            if (_extractor == null)
            {
                _lastError = "Not initialized";
                return IntPtr.Zero;
            }

            var names = _extractor.Schemas.Keys.OrderBy(x => x).ToArray();
            var arr = new JsonArray();
            foreach (var n in names) arr.Add((JsonNode?)n);
            var json = arr.ToJsonString();
            return AllocString(json);
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return IntPtr.Zero;
        }
    }

    /// <summary>
    ///     Get schema as JSON. Returned string must be freed.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_get_schema")]
    public static IntPtr GetSchema(IntPtr classNamePtr)
    {
        try
        {
            if (_extractor == null)
            {
                _lastError = "Not initialized";
                return IntPtr.Zero;
            }

            var name = Marshal.PtrToStringUTF8(classNamePtr)!;
            var schema = _extractor.FindSchema(name);
            if (schema == null)
            {
                _lastError = $"Schema not found: {name}";
                return IntPtr.Zero;
            }

            var result = new JsonObject
            {
                ["fullName"] = schema.FullName,
                ["name"] = schema.Name,
                ["namespace"] = schema.Namespace,
                ["isStringKeyed"] = schema.IsStringKeyed,
                ["isAbstract"] = schema.IsAbstract,
                ["fields"] = new JsonArray(schema.Fields.Select(f => (JsonNode)new JsonObject
                {
                    ["stringKey"] = f.StringKey,
                    ["intKey"] = f.IntKey,
                    ["memberName"] = f.MemberName,
                    ["typeName"] = f.TypeName
                }).ToArray())
            };
            return AllocString(result.ToJsonString());
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return IntPtr.Zero;
        }
    }

    /// <summary>
    ///     Decode msgpack binary to JSON string. Returned string must be freed.
    ///     data: pointer to msgpack bytes, dataLen: byte count.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_decode_json")]
    public static IntPtr DecodeJson(IntPtr classNamePtr, IntPtr data, int dataLen)
    {
        try
        {
            if (_codec == null)
            {
                _lastError = "Not initialized";
                return IntPtr.Zero;
            }

            var name = Marshal.PtrToStringUTF8(classNamePtr)!;
            var bytes = new byte[dataLen];
            Marshal.Copy(data, bytes, 0, dataLen);
            var json = _codec.DecodeJson(name, bytes);
            return AllocString(json == null ? "null" : json.ToJsonString());
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return IntPtr.Zero;
        }
    }

    /// <summary>
    ///     Encode JSON string to msgpack binary.
    ///     Returns pointer to allocated bytes; outLen receives the byte count.
    ///     Must be freed with msgpack_free_bytes.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_encode_json")]
    public static IntPtr EncodeJson(IntPtr classNamePtr, IntPtr jsonPtr, IntPtr outLenPtr)
    {
        try
        {
            if (_codec == null)
            {
                _lastError = "Not initialized";
                return IntPtr.Zero;
            }

            var name = Marshal.PtrToStringUTF8(classNamePtr)!;
            var jsonStr = Marshal.PtrToStringUTF8(jsonPtr)!;
            var json = JsonNode.Parse(jsonStr);
            var data = _codec.EncodeJson(name, json);

            var result = Marshal.AllocHGlobal(data.Length);
            Marshal.Copy(data, 0, result, data.Length);
            Marshal.WriteInt32(outLenPtr, data.Length);
            return result;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return IntPtr.Zero;
        }
    }

    /// <summary>
    ///     Decode compact msgpack binary to named-key msgpack (string-keyed map).
    ///     Returns pointer to allocated bytes; outLen receives the byte count.
    ///     Must be freed with msgpack_free_bytes.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_decode")]
    public static IntPtr Decode(IntPtr classNamePtr, IntPtr data, int dataLen, IntPtr outLenPtr)
    {
        try
        {
            if (_codec == null)
            {
                _lastError = "Not initialized";
                return IntPtr.Zero;
            }

            var name = Marshal.PtrToStringUTF8(classNamePtr)!;
            var bytes = new byte[dataLen];
            Marshal.Copy(data, bytes, 0, dataLen);
            var result = _codec.Decode(name, bytes);

            var ptr = Marshal.AllocHGlobal(result.Length);
            Marshal.Copy(result, 0, ptr, result.Length);
            Marshal.WriteInt32(outLenPtr, result.Length);
            return ptr;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return IntPtr.Zero;
        }
    }

    /// <summary>
    ///     Encode named-key msgpack (string-keyed map) to compact msgpack binary.
    ///     Returns pointer to allocated bytes; outLen receives the byte count.
    ///     Must be freed with msgpack_free_bytes.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_encode")]
    public static IntPtr Encode(IntPtr classNamePtr, IntPtr data, int dataLen, IntPtr outLenPtr)
    {
        try
        {
            if (_codec == null)
            {
                _lastError = "Not initialized";
                return IntPtr.Zero;
            }

            var name = Marshal.PtrToStringUTF8(classNamePtr)!;
            var bytes = new byte[dataLen];
            Marshal.Copy(data, bytes, 0, dataLen);
            var result = _codec.Encode(name, bytes);

            var ptr = Marshal.AllocHGlobal(result.Length);
            Marshal.Copy(result, 0, ptr, result.Length);
            Marshal.WriteInt32(outLenPtr, result.Length);
            return ptr;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return IntPtr.Zero;
        }
    }

    /// <summary>Export Avro schema JSON. Returned string must be freed.</summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_avro_schema")]
    public static IntPtr AvroSchema(IntPtr classNamePtr)
    {
        try
        {
            if (_extractor == null)
            {
                _lastError = "Not initialized";
                return IntPtr.Zero;
            }

            var name = Marshal.PtrToStringUTF8(classNamePtr)!;
            var schema = _extractor.FindSchema(name);
            if (schema == null)
            {
                _lastError = $"Schema not found: {name}";
                return IntPtr.Zero;
            }

            var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
            return AllocString(avro.ToJsonString());
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return IntPtr.Zero;
        }
    }

    /// <summary>
    ///     Export Avro schema with options. flags: bit 0 = nullable, bit 1 = flat.
    ///     Returned string must be freed.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_avro_schema_ex")]
    public static IntPtr AvroSchemaEx(IntPtr classNamePtr, int flags)
    {
        try
        {
            if (_extractor == null)
            {
                _lastError = "Not initialized";
                return IntPtr.Zero;
            }

            var name = Marshal.PtrToStringUTF8(classNamePtr)!;
            var schema = _extractor.FindSchema(name);
            if (schema == null)
            {
                _lastError = $"Schema not found: {name}";
                return IntPtr.Zero;
            }

            var options = new AvroExportOptions
            {
                AllNullable = (flags & 1) != 0,
                Flat = (flags & 2) != 0
            };
            var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas, options);
            return AllocString(avro.ToJsonString());
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            return IntPtr.Zero;
        }
    }

    /// <summary>
    ///     Allocate a buffer in native memory. Used by WASM callers (e.g. Go/wazero)
    ///     to write input strings into the module's linear memory before calling other functions.
    ///     The returned pointer must be freed with msgpack_free_bytes.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_alloc")]
    public static IntPtr Alloc(int size)
    {
        return size > 0 ? Marshal.AllocHGlobal(size) : IntPtr.Zero;
    }

    /// <summary>Free a string returned by this library.</summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_free_string")]
    public static void FreeString(IntPtr ptr)
    {
        if (ptr != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(ptr);
        }
    }

    /// <summary>Free bytes returned by msgpack_encode.</summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_free_bytes")]
    public static void FreeBytes(IntPtr ptr)
    {
        if (ptr != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(ptr);
        }
    }

    /// <summary>Shutdown and free all resources.</summary>
    [UnmanagedCallersOnly(EntryPoint = "msgpack_shutdown")]
    public static void Shutdown()
    {
        _codec = null;
        _extractor?.Dispose();
        _extractor = null;
    }

    private static IntPtr AllocString(string s)
    {
        var bytes = Encoding.UTF8.GetBytes(s);
        var ptr = Marshal.AllocHGlobal(bytes.Length + 1);
        Marshal.Copy(bytes, 0, ptr, bytes.Length);
        Marshal.WriteByte(ptr + bytes.Length, 0); // null terminator
        return ptr;
    }
}