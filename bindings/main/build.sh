export CGO_ENABLED=1
VERSION=${SOP_VERSION:-latest}
RELEASE_DIR="../../release"
mkdir -p $RELEASE_DIR

echo "Building AMD64 darwin"

export GOOS=darwin
export GOARCH=amd64
unset CC
go build -buildmode=c-shared -o ../python/sop/libjsondb_amd64darwin.dylib *.go
cp ../python/sop/libjsondb_amd64darwin.dylib ../csharp/Sop/
cp ../python/sop/libjsondb_amd64darwin.h ../csharp/Sop/
# For testing in Examples.
cp ../python/sop/libjsondb_amd64darwin.dylib ../csharp/Sop.Examples/libjsondb.dylib
cp ../python/sop/libjsondb_amd64darwin.h ../csharp/Sop.Examples/libjsondb.h
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/darwin-x86-64
cp ../python/sop/libjsondb_amd64darwin.dylib ../java/src/main/resources/darwin-x86-64/libjsondb.dylib

# Build Browser
echo "Building sop-browser for darwin/amd64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-browser-darwin-amd64 ../../tools/data_browser

rm -rf ../csharp/Sop.Examples/bin
rm -rf ../csharp/Sop.Examples/obj
rm -rf ../csharp/Sop.Examples/data

echo "Building AMD64 windows"

export GOOS=windows
export GOARCH=amd64
export CC=x86_64-w64-mingw32-gcc
go build -buildmode=c-shared -o ../python/sop/libjsondb_amd64windows.dll *.go
cp ../python/sop/libjsondb_amd64windows.dll ../csharp/Sop/
cp ../python/sop/libjsondb_amd64windows.h ../csharp/Sop/
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/win32-x86-64
cp ../python/sop/libjsondb_amd64windows.dll ../java/src/main/resources/win32-x86-64/libjsondb.dll

# Build Browser
echo "Building sop-browser for windows/amd64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-browser-windows-amd64.exe ../../tools/data_browser

echo "Building ARM64 darwin"

export GOOS=darwin
export GOARCH=arm64
unset CC
go build -buildmode=c-shared -o ../python/sop/libjsondb_arm64darwin.dylib *.go
cp ../python/sop/libjsondb_arm64darwin.dylib ../csharp/Sop/
cp ../python/sop/libjsondb_arm64darwin.h ../csharp/Sop/
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/darwin-aarch64
cp ../python/sop/libjsondb_arm64darwin.dylib ../java/src/main/resources/darwin-aarch64/libjsondb.dylib

# Build Browser
echo "Building sop-browser for darwin/arm64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-browser-darwin-arm64 ../../tools/data_browser

echo "Building AMD64 linux"

export GOOS=linux
export GOARCH=amd64
export CC="zig cc -target x86_64-linux-gnu"
go build -buildmode=c-shared -o ../python/sop/libjsondb_amd64linux.so *.go
cp ../python/sop/libjsondb_amd64linux.so ../csharp/Sop/
cp ../python/sop/libjsondb_amd64linux.h ../csharp/Sop/
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/linux-x86-64
cp ../python/sop/libjsondb_amd64linux.so ../java/src/main/resources/linux-x86-64/libjsondb.so

# Build Browser
echo "Building sop-browser for linux/amd64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-browser-linux-amd64 ../../tools/data_browser

echo "Building ARM64 linux"

export GOOS=linux
export GOARCH=arm64
export CC="zig cc -target aarch64-linux-gnu"
go build -buildmode=c-shared -o ../python/sop/libjsondb_arm64linux.so *.go
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/linux-aarch64
cp ../python/sop/libjsondb_arm64linux.so ../java/src/main/resources/linux-aarch64/libjsondb.so
cp ../python/sop/libjsondb_arm64linux.so ../csharp/Sop/
cp ../python/sop/libjsondb_arm64linux.h ../csharp/Sop/

# Build Browser
echo "Building sop-browser for linux/arm64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-browser-linux-arm64 ../../tools/data_browser

echo "Build complete."
