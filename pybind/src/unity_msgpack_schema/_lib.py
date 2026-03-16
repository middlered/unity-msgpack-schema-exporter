from __future__ import annotations

import ctypes
import json
import os
import platform
from ctypes import POINTER, c_char_p, c_int, c_void_p
from pathlib import Path
from typing import Any


def _load_lib(path: str) -> ctypes.CDLL:
    """Load the native library and configure all function signatures."""
    lib = ctypes.CDLL(path)

    # int msgpack_init(const char* path)
    lib.msgpack_init.restype = c_int
    lib.msgpack_init.argtypes = [c_char_p]

    # char* msgpack_get_error(void)
    lib.msgpack_get_error.restype = c_void_p
    lib.msgpack_get_error.argtypes = []

    # void msgpack_shutdown(void)
    lib.msgpack_shutdown.restype = None
    lib.msgpack_shutdown.argtypes = []

    # int msgpack_schema_count(void)
    lib.msgpack_schema_count.restype = c_int
    lib.msgpack_schema_count.argtypes = []

    # char* msgpack_list_classes(void)
    lib.msgpack_list_classes.restype = c_void_p
    lib.msgpack_list_classes.argtypes = []

    # char* msgpack_get_schema(const char* className)
    lib.msgpack_get_schema.restype = c_void_p
    lib.msgpack_get_schema.argtypes = [c_char_p]

    # char* msgpack_decode_json(const char* className, const void* data, int length)
    lib.msgpack_decode_json.restype = c_void_p
    lib.msgpack_decode_json.argtypes = [c_char_p, c_void_p, c_int]

    # void* msgpack_encode_json(const char* className, const char* json, int* outLength)
    lib.msgpack_encode_json.restype = c_void_p
    lib.msgpack_encode_json.argtypes = [c_char_p, c_char_p, POINTER(c_int)]

    # void* msgpack_decode(const char* className, const void* data, int inputLen, int* outLength)
    lib.msgpack_decode.restype = c_void_p
    lib.msgpack_decode.argtypes = [c_char_p, c_void_p, c_int, POINTER(c_int)]

    # void* msgpack_encode(const char* className, const void* data, int inputLen, int* outLength)
    lib.msgpack_encode.restype = c_void_p
    lib.msgpack_encode.argtypes = [c_char_p, c_void_p, c_int, POINTER(c_int)]

    # char* msgpack_avro_schema(const char* className)
    lib.msgpack_avro_schema.restype = c_void_p
    lib.msgpack_avro_schema.argtypes = [c_char_p]

    # char* msgpack_avro_schema_ex(const char* className, int flags)
    lib.msgpack_avro_schema_ex.restype = c_void_p
    lib.msgpack_avro_schema_ex.argtypes = [c_char_p, c_int]

    # void msgpack_free_string(void* ptr)
    lib.msgpack_free_string.restype = None
    lib.msgpack_free_string.argtypes = [c_void_p]

    # void msgpack_free_bytes(void* ptr)
    lib.msgpack_free_bytes.restype = None
    lib.msgpack_free_bytes.argtypes = [c_void_p]

    return lib


def _find_native_lib(lib_path: str | None = None) -> str:
    """Locate the NativeAOT shared library."""
    if lib_path and os.path.isfile(lib_path):
        return lib_path

    # Determine platform-specific library name
    system = platform.system().lower()
    if system == "linux":
        lib_name = "UnityMsgpackSchemaExporter.Native.so"
    elif system == "darwin":
        lib_name = "UnityMsgpackSchemaExporter.Native.dylib"
    elif system == "windows":
        lib_name = "UnityMsgpackSchemaExporter.Native.dll"
    else:
        lib_name = "UnityMsgpackSchemaExporter.Native.so"

    # Search common locations
    search_paths = [
        Path(__file__).parent,
        Path(__file__).parent / "lib",
    ]

    # Add platform-specific dist paths
    machine = platform.machine().lower()
    if machine in ("x86_64", "amd64"):
        arch = "x64"
    elif machine in ("aarch64", "arm64"):
        arch = "arm64"
    else:
        arch = machine

    rid = f"{system}-{arch}" if system != "darwin" else f"osx-{arch}"
    search_paths.append(Path.cwd() / "dist" / f"native-{rid}")

    for base_path in search_paths:
        candidate = base_path / lib_name
        if candidate.is_file():
            return str(candidate)

    # Check UNITY_MSGPACK_NATIVE_LIB env var
    env_path = os.environ.get("UNITY_MSGPACK_NATIVE_LIB")
    if env_path and os.path.isfile(env_path):
        return env_path

    raise FileNotFoundError(
        f"Could not find {lib_name}. "
        f"Set UNITY_MSGPACK_NATIVE_LIB env var or pass lib_path parameter. "
        f"Build with: dotnet publish UnityMsgpackSchemaExporter.Native -c Release -r {rid}"
    )


class MsgPackSchema:
    """
    Python interface to Unity MsgPack Schema Tools.

    Wraps the NativeAOT shared library via ctypes for schema extraction,
    MessagePack encoding/decoding, and Avro schema export.

    Args:
        dll_path: Path to the DummyDll directory or a single .dll file.
        lib_path: Optional path to the NativeAOT shared library.
                  If not provided, searches common locations.

    Example::

        with MsgPackSchema("/path/to/DummyDll") as schema:
            classes = schema.list_classes()
            decoded = schema.decode("MyMsgPack", binary_data)
    """

    _lib: ctypes.CDLL | None = None

    def __init__(self, dll_path: str, *, lib_path: str | None = None):
        self._closed = False
        native_lib_path = _find_native_lib(lib_path)
        self._lib = _load_lib(native_lib_path)

        result = self._lib.msgpack_init(dll_path.encode("utf-8"))
        if result != 0:
            error = self._get_error()
            raise RuntimeError(f"Failed to initialize: {error}")

    def _get_error(self) -> str:
        """Get last error message from the native library."""
        ptr = self._lib.msgpack_get_error()  # type: ignore[union-attr]
        if not ptr:
            return "unknown error"
        try:
            return ctypes.string_at(ptr).decode("utf-8")
        finally:
            self._lib.msgpack_free_string(ptr)  # type: ignore[union-attr]

    def _read_string(self, ptr: int) -> str:
        """Read a string from a native pointer and free it."""
        if not ptr:
            raise RuntimeError(self._get_error())
        try:
            return ctypes.string_at(ptr).decode("utf-8")
        finally:
            self._lib.msgpack_free_string(ptr)  # type: ignore[union-attr]

    @property
    def schema_count(self) -> int:
        """Number of loaded schemas."""
        self._check_open()
        return self._lib.msgpack_schema_count()  # type: ignore[union-attr]

    def list_classes(self) -> list[str]:
        """List all MessagePackObject class names."""
        self._check_open()
        ptr = self._lib.msgpack_list_classes()  # type: ignore[union-attr]
        json_str = self._read_string(ptr)
        return json.loads(json_str)

    def get_schema(self, class_name: str) -> dict[str, Any]:
        """Get schema for a specific class as a dictionary."""
        self._check_open()
        ptr = self._lib.msgpack_get_schema(class_name.encode("utf-8"))  # type: ignore[union-attr]
        json_str = self._read_string(ptr)
        return json.loads(json_str)

    def decode(self, class_name: str, data: bytes) -> bytes:
        """Decode compact msgpack binary to named-key msgpack (string-keyed map).

        Input is the compact indexed/keyless msgpack format used by
        MessagePack-CSharp.  Output is a standard msgpack map with field-name
        string keys, readable by any msgpack library.

        Example::

            named = schema.decode("MyClass", compact_bytes)
            obj = msgpack.unpackb(named)  # {'id': 42, 'name': 'foo', ...}
        """
        self._check_open()
        buf = (ctypes.c_char * len(data)).from_buffer_copy(data)
        out_len = c_int(0)
        ptr = self._lib.msgpack_decode(  # type: ignore[union-attr]
            class_name.encode("utf-8"), buf, len(data), ctypes.byref(out_len)
        )
        if not ptr:
            raise RuntimeError(self._get_error())
        try:
            return ctypes.string_at(ptr, out_len.value)
        finally:
            self._lib.msgpack_free_bytes(ptr)  # type: ignore[union-attr]

    def encode(self, class_name: str, data: bytes) -> bytes:
        """Encode named-key msgpack (string-keyed map) to compact msgpack binary.

        Input is a msgpack map produced by :meth:`decode` or any msgpack
        library.  Output is the compact indexed/keyless format expected by
        MessagePack-CSharp.

        Example::

            compact = schema.encode("MyClass", msgpack.packb({'id': 42, 'name': 'foo'}))
        """
        self._check_open()
        buf = (ctypes.c_char * len(data)).from_buffer_copy(data)
        out_len = c_int(0)
        ptr = self._lib.msgpack_encode(  # type: ignore[union-attr]
            class_name.encode("utf-8"), buf, len(data), ctypes.byref(out_len)
        )
        if not ptr:
            raise RuntimeError(self._get_error())
        try:
            return ctypes.string_at(ptr, out_len.value)
        finally:
            self._lib.msgpack_free_bytes(ptr)  # type: ignore[union-attr]

    def decode_json(self, class_name: str, data: bytes) -> dict[str, Any]:
        """Decode compact msgpack binary to a named-key dict (via JSON).

        Convenience wrapper that returns a Python dict instead of msgpack bytes.
        Use :meth:`decode` for the more efficient msgpack->msgpack path.
        """
        self._check_open()
        buf = (ctypes.c_char * len(data)).from_buffer_copy(data)
        ptr = self._lib.msgpack_decode_json(  # type: ignore[union-attr]
            class_name.encode("utf-8"), buf, len(data)
        )
        json_str = self._read_string(ptr)
        return json.loads(json_str)

    def encode_json(self, class_name: str, obj: dict[str, Any]) -> bytes:
        """Encode a named-key dict to compact msgpack binary (via JSON).

        Convenience wrapper that accepts a Python dict instead of msgpack bytes.
        Use :meth:`encode` for the more efficient msgpack->msgpack path.
        """
        self._check_open()
        json_bytes = json.dumps(obj).encode("utf-8")
        out_len = c_int(0)
        ptr = self._lib.msgpack_encode_json(  # type: ignore[union-attr]
            class_name.encode("utf-8"), json_bytes, ctypes.byref(out_len)
        )
        if not ptr:
            raise RuntimeError(self._get_error())
        try:
            return ctypes.string_at(ptr, out_len.value)
        finally:
            self._lib.msgpack_free_bytes(ptr)  # type: ignore[union-attr]

    def avro_schema(
        self,
        class_name: str,
        *,
        nullable: bool = False,
        flat: bool = False,
    ) -> dict[str, Any]:
        """Export Avro schema for a class."""
        self._check_open()
        if nullable or flat:
            flags = (1 if nullable else 0) | (2 if flat else 0)
            ptr = self._lib.msgpack_avro_schema_ex(class_name.encode("utf-8"), flags)  # type: ignore[union-attr]
        else:
            ptr = self._lib.msgpack_avro_schema(class_name.encode("utf-8"))  # type: ignore[union-attr]
        json_str = self._read_string(ptr)
        return json.loads(json_str)

    def close(self) -> None:
        """Shut down and release all native resources."""
        if not getattr(self, "_closed", True):
            self._lib.msgpack_shutdown()  # type: ignore[union-attr]
            self._closed = True

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("MsgPackSchema is closed")

    def __enter__(self) -> MsgPackSchema:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()
