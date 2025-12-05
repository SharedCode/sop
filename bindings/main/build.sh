export CGO_ENABLED=1
export GOOS=windows
export GOARCH=amd64
export CC=x86_64-w64-mingw32-gcc
go build -buildmode=c-shared -o ../python/sop/libjsondb_amd64windows.dll *.go

export GOOS=darwin
export GOARCH=amd64
unset CC
go build -buildmode=c-shared -o ../python/sop/libjsondb_amd64darwin.dylib *.go

export GOOS=darwin
export GOARCH=arm64
go build -buildmode=c-shared -o ../python/sop/libjsondb_arm64darwin.dylib *.go

export GOOS=linux
export GOARCH=amd64
export CC="zig cc -target x86_64-linux-gnu"
go build -buildmode=c-shared -o ../python/sop/libjsondb_amd64linux.so *.go

export GOOS=linux
export GOARCH=arm64
export CC="zig cc -target aarch64-linux-gnu"
go build -buildmode=c-shared -o ../python/sop/libjsondb_arm64linux.so *.go
