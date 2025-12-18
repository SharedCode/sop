# Packaging SOP for Multiple Languages

SOP is a polyglot library backed by a high-performance Go engine. To support multiple languages (Python, C#, Java, Rust), we compile the Go code into a C-shared library (`.so`, `.dll`, `.dylib`) and bundle it with the language-specific bindings.

## General Strategy

1.  **Compile Go Core**: We use `go build -buildmode=c-shared` to generate shared libraries for all supported targets (Linux x64/ARM64, macOS x64/ARM64, Windows x64).
2.  **Bundle**: These binaries are copied into the package structure of the target language.
3.  **Load**: The language binding loads the appropriate binary at runtime based on the current OS and Architecture.

## Language-Specific Details

### 🐍 Python (Completed)
*   **Mechanism**: Python Wheels (`.whl`) and Source Distribution (`.tar.gz`).
*   **Strategy**: We use **Platform-Specific Wheels**.
    *   We build separate wheels for macOS (x64/ARM64), Linux (x64/ARM64), and Windows.
    *   Each wheel contains *only* the shared library for that specific platform, keeping the install size small (~4-7MB).
    *   We also provide a "Fat" Source Distribution (sdist) containing *all* binaries (~28MB) as a fallback for unsupported platforms.
*   **Structure**: The shared libraries are placed inside the `sop` package directory.
*   **Loading**: `sop/call_go.py` detects the OS/Arch and loads the correct file using `ctypes.cdll.LoadLibrary`.
*   **Build**: `bindings/python/build_wheels.sh` handles this.

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

### 🦀 Rust (Completed)
*   **Mechanism**: Cargo / Crates.io.
*   **Strategy**: **Static Linking**.
    *   We build static archives (`.a`) for each platform using `go build -buildmode=c-archive`.
    *   These are placed in `bindings/rust/lib/`.
    *   `build.rs` detects the OS/Arch and links the correct static library.
*   **Build**: `bindings/main/build.sh` generates the static libraries.

## Release Workflow & Build Automation

We use a **"One Stop Hub"** strategy for building release artifacts. The goal is to ensure that all language bindings and auxiliary tools (like `sop-httpserver`) are built consistently with the same version.

### The Master Build Script (`bindings/main/build.sh`)
This script is the central engine. It:
1.  Builds the **Core Shared Libraries** (`libjsondb`) for all platforms (macOS, Linux, Windows).
2.  Builds the **Core Static Libraries** (`libjsondb.a`) for Rust.
3.  Builds the **SOP Data Browser** (`sop-httpserver`) for all platforms.
4.  Distributes these binaries to the appropriate language folders (`python/sop`, `csharp/Sop`, `java/resources`, `rust/lib`).
5.  Outputs standalone tools to a `release/` directory in the project root.

### 🐳 Hybrid Build System (Recommended)
To ensure consistent builds across all platforms while maintaining native compatibility for macOS, we use a **Hybrid Build System**.

1.  **Run**: `./bindings/build_in_docker.sh`
2.  **What it does**:
    *   **Linux & Windows**: Builds inside a Docker container (using Go 1.24, Zig, and MinGW) to ensure reproducibility and broad compatibility.
    *   **macOS**: Builds **locally** on your host machine (if you are on macOS) using native Apple tools (Xcode/Clang). This avoids cross-compilation bugs and linker issues.
    *   **Artifact Collection**: Automatically copies all artifacts (from Docker and Local builds) into the correct `bindings/` folders on your host.

This single command handles the entire complexity of cross-platform compilation.

### Release Process (Python Example)

The Python build script (`bindings/python/build_wheels.sh`) acts as the orchestrator for a release.

1.  **Update Version**: Bump the version in `bindings/python/pyproject.toml`.
2.  **Run Build**: Execute `./bindings/python/build_wheels.sh`.
    *   It cleans previous builds.
    *   It builds the **Source Distribution (sdist)** containing ALL binaries (fallback).
    *   It loops through target platforms (macOS, Linux, Windows) and builds **Platform-Specific Wheels** containing only the relevant binary.
3.  **Publish Python**: `python3 -m twine upload dist/*`
    *   This uploads all the optimized wheels and the fallback sdist to PyPI.
    *   `pip install` will automatically choose the best (smallest) wheel for the user's platform.
4.  **Publish Tools**:
    *   Go to the [GitHub Releases](https://github.com/sharedcode/sop/releases) page.
    *   Create a new release tag (e.g., `sop4py-v2.0.34`).
    *   Upload the binaries found in the `release/` folder (e.g., `sop-httpserver-darwin-arm64`, `sop-httpserver-windows-amd64.exe`).

This ensures that users who install the Python package can download the exact matching version of the `sop-httpserver` tool.

### Release Process (C# / .NET)

1.  **Update Version**: Update the version number in `bindings/csharp/VERSION`.
2.  **Build Native Libs**: Ensure you have run `./bindings/build_in_docker.sh` (or `build_local_macos.sh`) so that the `libjsondb` binaries are present in `bindings/csharp/Sop/`.
3.  **Build & Pack**: Run the C# build script. This will update the project files with the new version and generate the NuGet packages.
    ```bash
    ./bindings/csharp/build.sh
    ```
    The packages will be output to `bindings/csharp/dist/`.
4.  **Test Locally (Recommended)**: Before pushing to NuGet, verify the package works.
    *   Install the tool from the local `dist` folder:
        ```bash
        # Uninstall previous version if needed
        dotnet tool uninstall -g Sop4CS.CLI
        
        # Install from local dist folder
        dotnet tool install -g Sop4CS.CLI --add-source ./bindings/csharp/dist --version <YOUR_VERSION>
        ```
    *   Run `sop-cli` to verify.
    *   Run `sop-cli httpserver` to verify the HTTP Server launcher.
5.  **Publish**:
    ```bash
    dotnet nuget push bindings/csharp/dist/Sop4CS.<VERSION>.nupkg --api-key <KEY> --source https://api.nuget.org/v3/index.json
    dotnet nuget push bindings/csharp/dist/Sop4CS.CLI.<VERSION>.nupkg --api-key <KEY> --source https://api.nuget.org/v3/index.json
    ```
