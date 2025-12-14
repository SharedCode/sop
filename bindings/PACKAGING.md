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
