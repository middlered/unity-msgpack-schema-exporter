# NativeAOT / FFI API

A shared library exposing C ABI for use from any language that supports FFI (Python, Rust, Go, C, etc.).

## Function Reference

```c
// Initialize: load schemas from a directory or single .dll file.
// Returns 0 on success, non-zero on failure.
int msgpack_init(const char* path);

// Get the last error message (must be freed with msgpack_free_string).
char* msgpack_get_error(void);

// Shut down and release all resources.
void msgpack_shutdown(void);

// Number of loaded schemas.
int msgpack_schema_count(void);

// JSON array of class names (must be freed with msgpack_free_string).
char* msgpack_list_classes(void);

// JSON schema for a class (must be freed with msgpack_free_string).
char* msgpack_get_schema(const char* className);

// Decode compact (indexed/keyless) msgpack -> named-key msgpack map.
// outLength is set to the number of bytes written.
// Returns NULL on error. Must be freed with msgpack_free_bytes.
void* msgpack_decode(const char* className,
                     const void* data, int inputLen,
                     int* outLength);

// Encode named-key msgpack map -> compact (indexed/keyless) msgpack.
// outLength is set to the number of bytes written.
// Returns NULL on error. Must be freed with msgpack_free_bytes.
void* msgpack_encode(const char* className,
                     const void* data, int inputLen,
                     int* outLength);

// Decode compact msgpack -> named-key JSON string.
// Returns NULL on error. Must be freed with msgpack_free_string.
char* msgpack_decode_json(const char* className, const void* data, int length);

// Encode named-key JSON string -> compact msgpack binary.
// outLength is set to the number of bytes written.
// Returns NULL on error. Must be freed with msgpack_free_bytes.
void* msgpack_encode_json(const char* className, const char* json, int* outLength);

// Export Avro schema JSON (nested mode).
// Must be freed with msgpack_free_string.
char* msgpack_avro_schema(const char* className);

// Export Avro schema JSON with options flags.
// flags: bit 0 = AllNullable, bit 1 = Flat
// Must be freed with msgpack_free_string.
char* msgpack_avro_schema_ex(const char* className, int flags);

// Free a string returned by the library.
void msgpack_free_string(void* ptr);

// Free binary data returned by msgpack_encode / msgpack_decode.
void msgpack_free_bytes(void* ptr);
```

## Encode/Decode

|                       | Input             | Output            | 
|-----------------------|-------------------|-------------------|
| `msgpack_decode`      | compact msgpack   | named-key msgpack |
| `msgpack_encode`      | named-key msgpack | compact msgpack   |
| `msgpack_decode_json` | compact msgpack   | `char*` JSON      |
| `msgpack_encode_json` | `char*` JSON      | compact msgpack   |

The compact format is what MessagePack-CSharp produces: fields stored as an array (integer keys) or map (string keys)
without names.
The named-key format is a standard msgpack map with field-name string keys readable by any msgpack library.
