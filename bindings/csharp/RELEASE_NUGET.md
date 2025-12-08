# Releasing SOP for C# to NuGet

This guide describes how to package and release the `Sop` C# binding to NuGet.

## Prerequisites

1.  **Go Toolchain**: To build the native library.
2.  **.NET SDK**: To pack the C# project.
3.  **NuGet API Key**: For pushing to nuget.org.

## 1. Build Native Libraries

The NuGet package must include the compiled Go library (`libjsondb`) for all supported platforms. You need to cross-compile these.

Create a folder structure like this in `bindings/csharp/Sop/`:

```
runtimes/
  osx-x64/
    native/
      libjsondb.dylib
  linux-x64/
    native/
      libjsondb.so
  win-x64/
    native/
      libjsondb.dll
```

**Build Commands (run from repo root):**

```bash
# macOS (x64)
GOOS=darwin GOARCH=amd64 go build -buildmode=c-shared -o bindings/csharp/Sop/runtimes/osx-x64/native/libjsondb.dylib ./bindings/main/...

# Linux (x64)
GOOS=linux GOARCH=amd64 go build -buildmode=c-shared -o bindings/csharp/Sop/runtimes/linux-x64/native/libjsondb.so ./bindings/main/...

# Windows (x64)
GOOS=windows GOARCH=amd64 go build -buildmode=c-shared -o bindings/csharp/Sop/runtimes/win-x64/native/libjsondb.dll ./bindings/main/...
```

> **Note**: Cross-compiling CGO (which `c-shared` requires) can be tricky. You might need a cross-compiler toolchain (like `zig cc` or `xgo`) if you are not building on the target OS.

## 2. Update Project File

Ensure `bindings/csharp/Sop/Sop.csproj` includes the native assets.

Add this to the `.csproj`:

```xml
<ItemGroup>
  <Content Include="runtimes\**\*.*">
    <Pack>true</Pack>
    <PackagePath>runtimes</PackagePath>
    <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
  </Content>
</ItemGroup>
```

And update the package metadata:

```xml
<PropertyGroup>
  <PackageId>Sop.Data</PackageId>
  <Version>1.0.0</Version>
  <Authors>SharedCode</Authors>
  <Description>Scalable Objects Persistence (SOP) - High-performance transactional storage engine.</Description>
  <PackageTags>database;btree;vector;storage;transactional</PackageTags>
</PropertyGroup>
```

## 3. Pack the Project

Run the pack command to create the `.nupkg` file:

```bash
dotnet pack bindings/csharp/Sop/Sop.csproj -c Release
```

This will generate a file like `bindings/csharp/Sop/bin/Release/Sop.Data.1.0.0.nupkg`.

## 4. Push to NuGet

Upload the package to NuGet.org:

```bash
dotnet nuget push bindings/csharp/Sop/bin/Release/Sop.Data.1.0.0.nupkg --api-key YOUR_API_KEY --source https://api.nuget.org/v3/index.json
```
