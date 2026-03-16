"""
Pytest configuration and fixtures for unity_msgpack_schema tests.
"""

import os
import platform
from pathlib import Path

import pytest


def _find_native_lib() -> str | None:
    """Find the NativeAOT shared library, building if necessary."""
    project_root = Path(__file__).parent.parent.parent  # pybind/../

    system = platform.system().lower()
    machine = platform.machine().lower()
    if machine in ("x86_64", "amd64"):
        arch = "x64"
    elif machine in ("aarch64", "arm64"):
        arch = "arm64"
    else:
        arch = machine

    if system == "linux":
        lib_name = "UnityMsgpackSchemaExporter.Native.so"
        rid = f"linux-{arch}"
    elif system == "darwin":
        lib_name = "UnityMsgpackSchemaExporter.Native.dylib"
        rid = f"osx-{arch}"
    elif system == "windows":
        lib_name = "UnityMsgpackSchemaExporter.Native.dll"
        rid = f"win-{arch}"
    else:
        return None

    dist_path = project_root / "dist" / f"native-{rid}" / lib_name
    if dist_path.is_file():
        return str(dist_path)

    # Check env var
    env_lib = os.environ.get("UNITY_MSGPACK_NATIVE_LIB")
    if env_lib and os.path.isfile(env_lib):
        return env_lib

    return None


def _find_test_dll() -> str | None:
    """Find the TestSchemas DLL for testing."""
    project_root = Path(__file__).parent.parent.parent

    # Look for the test schemas DLL in build output
    candidates = list(
        project_root.glob(
            "UnityMsgpackSchemaExporter.TestSchemas/bin/Release/*/UnityMsgpackSchemaExporter.TestSchemas.dll"
        )
    )
    if candidates:
        return str(candidates[0])

    return None


@pytest.fixture(scope="session")
def native_lib_path() -> str:
    """Provide the path to the NativeAOT shared library."""
    path = _find_native_lib()
    if path is None:
        pytest.skip(
            "NativeAOT library not found. Build with: "
            "dotnet publish UnityMsgpackSchemaExporter.Native -c Release -r <rid>"
        )
    return path


@pytest.fixture(scope="session")
def test_dll_path() -> str:
    """Provide the path to the TestSchemas DLL."""
    path = _find_test_dll()
    if path is None:
        pytest.skip(
            "TestSchemas DLL not found. Build with: "
            "dotnet build UnityMsgpackSchemaExporter.Tests -c Release"
        )
    return path
