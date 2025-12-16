# Packaging SOP for Multiple Languages

SOP is a polyglot library backed by a high-performance Go engine. To support multiple languages (Python, C#, Java, Rust), we compile the Go code into a C-shared library (`.so`, `.dll`, `.dylib`) and bundle it with the language-specific bindings.

## General Strategy

1.  **Compile Go Core**: We use `go build -buildmode=c-shared` to generate shared libraries for all supported targets (Linux x64/ARM64, macOS x64/ARM64, Windows x64).
2.  **Bundle**: These binaries are copied into the package structure of the target language.
3.  **Load**: The language binding loads the appropriate binary at runtime based on the current OS and Architecture.

## Language-Specific Details

### 🐍 Python (Completed)
*   **Mechanism**: Python Wheels (`.whl`).
*   **Structure**: The shared libraries are placed inside the `sop` package directory.
*   **Loading**: `sop/call_go.py` detects the OS/Arch and loads the correct file using `ctypes.cdll.LoadLibrary`.
*   **Build**: `bindings/python/build.sh` handles this.

### ♯ C# / .NET (Completed)
*   **Mechanism**: NuGet Packages (`.nupkg`).
*   **Structure**: We use the standard `runtimes/` folder structure supported by .NET Core / .NET 5+.
    *   `runtimes/win-x64/native/libjsondb.dll`
    *   `runtimes/linux-x64/native/libjsondb.so`
    *   `runtimes/osx-arm64/native/libjsondb.dylib`
*   **Configuration**: `Sop.csproj` includes these files with `<PackagePath>runtimes/...</PackagePath>`.
*   **Loading**: .NET automatically loads the correct native library from the `runtimes` folder via `DllImport` (or `NativeLibrary.Load`).

### ☕ Java (Completed)
*   **Mechanism**: JAR / Maven.
*   **Structure**: JNA (Java Native Access) looks for libraries in the classpath under `darwin-aarch64/`, `linux-x86-64/`, etc.
*   **Build**: `bindings/main/build.sh` now creates `src/main/resources` and copies the shared libraries into the correct JNA platform folders.
*   **Loading**: When packaged into a JAR, JNA automatically extracts and loads the correct library from these resource folders.

### 🦀 Rust (To Do)
*   **Mechanism**: Cargo / Crates.io.
*   **Current State**: Links dynamically to the local `../main` folder (good for dev, bad for distribution).
*   **Distribution Strategy**:
    *   **Option A (Static Linking - Recommended)**: Build a static archive (`.a`) from Go (`-buildmode=c-archive`) and link it statically in `build.rs`. This produces a single standalone Rust binary.
    *   **Option B (Build from Source)**: Have `build.rs` invoke `go build` to compile the library on the user's machine. Requires the user to have Go installed.

## Release Workflow & Build Automation

We use a **"One Stop Hub"** strategy for building release artifacts. The goal is to ensure that all language bindings and auxiliary tools (like `sop-browser`) are built consistently with the same version.

### The Master Build Script (`bindings/main/build.sh`)
This script is the central engine. It:
1.  Builds the **Core Shared Libraries** (`libjsondb`) for all platforms (macOS, Linux, Windows).
2.  Builds the **SOP Data Browser** (`sop-browser`) for all platforms.
3.  Distributes these binaries to the appropriate language folders (`python/sop`, `csharp/Sop`, `java/resources`).
4.  Outputs standalone tools to a `release/` directory in the project root.

### Release Process (Python Example)

The Python build script (`bindings/python/build.sh`) acts as the orchestrator for a release.

1.  **Update Version**: Bump the version in `bindings/python/pyproject.toml`.
2.  **Run Build**: Execute `./bindings/python/build.sh`.
    *   It extracts the version from `pyproject.toml`.
    *   It calls the Master Build Script (`bindings/main/build.sh`) with `SOP_VERSION` set.
    *   It builds the Python wheels.
3.  **Publish Python**: `python3 -m twine upload dist/*`
4.  **Publish Tools**:
    *   Go to the [GitHub Releases](https://github.com/sharedcode/sop/releases) page.
    *   Create a new release tag (e.g., `sop4py-v2.0.32`).
    *   Upload the binaries found in the `release/` folder (e.g., `sop-browser-darwin-arm64`, `sop-browser-windows-amd64.exe`).

This ensures that users who install the Python package can download the exact matching version of the `sop-browser` tool.
