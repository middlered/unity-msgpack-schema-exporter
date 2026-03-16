//! Rust FFI example: calling UnityMsgpackSchemaExporter native shared library.
//!
//! Prerequisites:
//!   Build the native library (Linux):
//!     cd ../../ && dotnet publish UnityMsgpackSchemaExporter.Native \
//!       -c Release -r linux-x64 -f net8.0 \
//!       -o dist/native-linux-x64
//!
//!   Run:
//!     cargo run -- <path/to/TestSchemas.dll>
//!
//!   The shared library is looked up in:
//!     ../../dist/native-linux-x64/libUnityMsgpackSchemaExporter.Native.so  (Linux)
//!     ../../dist/native-win-x64/UnityMsgpackSchemaExporter.Native.dll       (Windows)
//!     ../../dist/native-osx-arm64/libUnityMsgpackSchemaExporter.Native.dylib (macOS)

use libloading::Library;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::{env, slice};

// ── Raw C-ABI function types ────────────────────────────────────────────────────

type FnInit         = unsafe extern "C" fn(*const c_char) -> c_int;
type FnGetError     = unsafe extern "C" fn() -> *mut c_char;
type FnSchemaCount  = unsafe extern "C" fn() -> c_int;
type FnListClasses  = unsafe extern "C" fn() -> *mut c_char;
type FnDecodeJson   = unsafe extern "C" fn(*const c_char, *const u8, c_int) -> *mut c_char;
type FnEncodeJson   = unsafe extern "C" fn(*const c_char, *const c_char, *mut c_int) -> *mut u8;
type FnDecode       = unsafe extern "C" fn(*const c_char, *const u8, c_int, *mut c_int) -> *mut u8;
type FnEncode       = unsafe extern "C" fn(*const c_char, *const u8, c_int, *mut c_int) -> *mut u8;
type FnAvroSchema   = unsafe extern "C" fn(*const c_char) -> *mut c_char;
type FnFreeString   = unsafe extern "C" fn(*mut c_char);
type FnFreeBytes    = unsafe extern "C" fn(*mut u8);
type FnShutdown     = unsafe extern "C" fn();

// ── Safe wrapper ───────────────────────────────────────────────────────────────

/// Stores raw function pointers loaded from the native .so.
/// The `_lib` field keeps the shared library loaded for the lifetime of this struct.
struct MsgpackLib {
    _lib:         Library,
    init:         FnInit,
    get_error:    FnGetError,
    schema_count: FnSchemaCount,
    list_classes: FnListClasses,
    decode_json:  FnDecodeJson,
    encode_json:  FnEncodeJson,
    decode:       FnDecode,
    encode:       FnEncode,
    avro_schema:  FnAvroSchema,
    free_string:  FnFreeString,
    free_bytes:   FnFreeBytes,
    shutdown:     FnShutdown,
}

macro_rules! load_fn {
    ($lib:expr, $name:literal, $ty:ty) => {{
        *$lib.get::<$ty>($name)
            .map_err(|e| format!("symbol {}: {e}", std::str::from_utf8($name).unwrap()))?
    }};
}

impl MsgpackLib {
    fn load(lib_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let lib = unsafe { Library::new(lib_path)? };
        Ok(MsgpackLib {
            init:         unsafe { load_fn!(lib, b"msgpack_init\0",         FnInit) },
            get_error:    unsafe { load_fn!(lib, b"msgpack_get_error\0",    FnGetError) },
            schema_count: unsafe { load_fn!(lib, b"msgpack_schema_count\0", FnSchemaCount) },
            list_classes: unsafe { load_fn!(lib, b"msgpack_list_classes\0", FnListClasses) },
            decode_json:  unsafe { load_fn!(lib, b"msgpack_decode_json\0",  FnDecodeJson) },
            encode_json:  unsafe { load_fn!(lib, b"msgpack_encode_json\0",  FnEncodeJson) },
            decode:       unsafe { load_fn!(lib, b"msgpack_decode\0",       FnDecode) },
            encode:       unsafe { load_fn!(lib, b"msgpack_encode\0",       FnEncode) },
            avro_schema:  unsafe { load_fn!(lib, b"msgpack_avro_schema\0",  FnAvroSchema) },
            free_string:  unsafe { load_fn!(lib, b"msgpack_free_string\0",  FnFreeString) },
            free_bytes:   unsafe { load_fn!(lib, b"msgpack_free_bytes\0",   FnFreeBytes) },
            shutdown:     unsafe { load_fn!(lib, b"msgpack_shutdown\0",     FnShutdown) },
            _lib: lib,
        })
    }

    fn init_schema(&self, path: &str) -> Result<(), String> {
        let cpath = CString::new(path).unwrap();
        let rc = unsafe { (self.init)(cpath.as_ptr()) };
        if rc != 0 { Err(self.last_error()) } else { Ok(()) }
    }

    fn last_error(&self) -> String {
        unsafe {
            let ptr = (self.get_error)();
            let msg = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            (self.free_string)(ptr);
            msg
        }
    }

    fn schema_count(&self) -> i32 { unsafe { (self.schema_count)() } }

    fn list_classes(&self) -> String {
        unsafe {
            let ptr = (self.list_classes)();
            if ptr.is_null() { return self.last_error(); }
            let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            (self.free_string)(ptr);
            s
        }
    }

    fn decode_json(&self, class_name: &str, data: &[u8]) -> Result<String, String> {
        let cname = CString::new(class_name).unwrap();
        unsafe {
            let ptr = (self.decode_json)(cname.as_ptr(), data.as_ptr(), data.len() as c_int);
            if ptr.is_null() { return Err(self.last_error()); }
            let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            (self.free_string)(ptr);
            Ok(s)
        }
    }

    fn encode_json(&self, class_name: &str, json: &str) -> Result<Vec<u8>, String> {
        let cname = CString::new(class_name).unwrap();
        let cjson = CString::new(json).unwrap();
        let mut out_len: c_int = 0;
        unsafe {
            let ptr = (self.encode_json)(cname.as_ptr(), cjson.as_ptr(), &mut out_len);
            if ptr.is_null() { return Err(self.last_error()); }
            let bytes = slice::from_raw_parts(ptr, out_len as usize).to_vec();
            (self.free_bytes)(ptr);
            Ok(bytes)
        }
    }

    fn decode(&self, class_name: &str, data: &[u8]) -> Result<Vec<u8>, String> {
        let cname = CString::new(class_name).unwrap();
        let mut out_len: c_int = 0;
        unsafe {
            let ptr = (self.decode)(cname.as_ptr(), data.as_ptr(), data.len() as c_int, &mut out_len);
            if ptr.is_null() { return Err(self.last_error()); }
            let bytes = slice::from_raw_parts(ptr, out_len as usize).to_vec();
            (self.free_bytes)(ptr);
            Ok(bytes)
        }
    }

    fn encode(&self, class_name: &str, named: &[u8]) -> Result<Vec<u8>, String> {
        let cname = CString::new(class_name).unwrap();
        let mut out_len: c_int = 0;
        unsafe {
            let ptr = (self.encode)(cname.as_ptr(), named.as_ptr(), named.len() as c_int, &mut out_len);
            if ptr.is_null() { return Err(self.last_error()); }
            let bytes = slice::from_raw_parts(ptr, out_len as usize).to_vec();
            (self.free_bytes)(ptr);
            Ok(bytes)
        }
    }

    fn avro_schema(&self, class_name: &str) -> Result<String, String> {
        let cname = CString::new(class_name).unwrap();
        unsafe {
            let ptr = (self.avro_schema)(cname.as_ptr());
            if ptr.is_null() { return Err(self.last_error()); }
            let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            (self.free_string)(ptr);
            Ok(s)
        }
    }
}

impl Drop for MsgpackLib {
    fn drop(&mut self) { unsafe { (self.shutdown)() }; }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn detect_lib_path() -> &'static str {
    if cfg!(target_os = "windows") {
        "../../dist/native-win-x64/UnityMsgpackSchemaExporter.Native.dll"
    } else if cfg!(target_os = "macos") {
        "../../dist/native-osx-arm64/UnityMsgpackSchemaExporter.Native.dylib"
    } else {
        "../../dist/native-linux-x64/UnityMsgpackSchemaExporter.Native.so"
    }
}

// ── Main ───────────────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema_dll = env::args().nth(1).unwrap_or_else(|| {
        "../../UnityMsgpackSchemaExporter.Tests/bin/Release/net8.0/UnityMsgpackSchemaExporter.TestSchemas.dll".into()
    });
    let lib_path = env::args().nth(2).unwrap_or_else(|| detect_lib_path().into());

    println!("Loading library: {lib_path}");
    let lib = MsgpackLib::load(&lib_path)?;

    // ── Init ─────────────────────────────────────────────────────────────────
    lib.init_schema(&schema_dll)?;
    println!("Loaded {} schemas", lib.schema_count());
    println!("Classes: {}", lib.list_classes());
    println!();

    // ── Example 1: Compact msgpack -> JSON ────────────────────────────────────
    // SimpleItem compact bytes from the C# example
    let compact = hex::decode("85A6616374697665C3A862696756616C7565CD270FA2696401A46E616D65A553776F7264A573636F7265CA41180000")?;

    println!("=== Example 1: Compact msgpack -> JSON ===");
    let json = lib.decode_json("UnityMsgpackSchemaExporter.TestSchemas.SimpleItem", &compact)?;
    println!("JSON: {json}");
    println!();

    // ── Example 2: JSON -> compact (round-trip) ────────────────────────────────
    println!("=== Example 2: JSON -> compact (round-trip) ===");
    let re_encoded = lib.encode_json("UnityMsgpackSchemaExporter.TestSchemas.SimpleItem", &json)?;
    println!("Re-encoded: {}", hex::encode(&re_encoded).to_uppercase());
    println!("Match: {}", compact == re_encoded);
    println!();

    // ── Example 3: Compact -> named -> compact ─────────────────────────────────
    println!("=== Example 3: Compact -> named-key -> compact ===");
    let named = lib.decode("UnityMsgpackSchemaExporter.TestSchemas.SimpleItem", &compact)?;
    println!("Named msgpack: {}", hex::encode(&named).to_uppercase());
    let re_compact = lib.encode("UnityMsgpackSchemaExporter.TestSchemas.SimpleItem", &named)?;
    println!("Re-compact:    {}", hex::encode(&re_compact).to_uppercase());
    println!("Match: {}", compact == re_compact);
    println!();

    // ── Example 4: Encode JSON -> compact msgpack ─────────────────────────────
    println!("=== Example 4: Encode JSON -> compact msgpack ===");
    let item_json = r#"{"id":2,"name":"Shield","score":7.5,"active":false,"bigValue":1234}"#;
    println!("Input JSON: {item_json}");
    let encoded = lib.encode_json("UnityMsgpackSchemaExporter.TestSchemas.SimpleItem", item_json)?;
    println!("Encoded compact: {}", hex::encode(&encoded).to_uppercase());

    // Verify by decoding it back.
    let decoded_back = lib.decode_json("UnityMsgpackSchemaExporter.TestSchemas.SimpleItem", &encoded)?;
    println!("Decoded back:    {decoded_back}");
    println!();

    // ── Example 5: Encode + decode IntKeyedItem (int-keyed array layout) ───────
    println!("=== Example 5: Encode + decode IntKeyedItem (int-keyed) ===");
    let int_item_json = r#"{"id":42,"label":"Axe","value":1.5}"#;
    println!("Input JSON: {int_item_json}");
    let int_encoded = lib.encode_json("UnityMsgpackSchemaExporter.TestSchemas.IntKeyedItem", int_item_json)?;
    println!("Encoded compact: {}", hex::encode(&int_encoded).to_uppercase());
    let int_decoded = lib.decode_json("UnityMsgpackSchemaExporter.TestSchemas.IntKeyedItem", &int_encoded)?;
    println!("Decoded back:    {int_decoded}");
    println!();

    // ── Example 6: Avro schema ────────────────────────────────────────────────
    println!("=== Example 6: Avro schema ===");
    let avro = lib.avro_schema("UnityMsgpackSchemaExporter.TestSchemas.SimpleItem")?;
    println!("Avro: {avro}");

    Ok(())
}
