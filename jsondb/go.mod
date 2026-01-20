module github.com/sharedcode/sop/jsondb

go 1.24.3

replace github.com/sharedcode/sop => ../

replace github.com/sharedcode/sop/infs => ../infs

replace github.com/sharedcode/sop/adapters/redis => ../adapters/redis

replace github.com/sharedcode/sop/adapters/cassandra => ../adapters/cassandra

require (
	github.com/google/uuid v1.6.0
	github.com/sharedcode/sop v0.0.0
)

require (
	cel.dev/expr v0.23.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/google/cel-go v0.25.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.12.4 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	github.com/sharedcode/sop/infs v0.0.0-00010101000000-000000000000 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	golang.org/x/exp v0.0.0-20230515195305-f3d0a9c9a5cc // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/protobuf v1.36.9 // indirect
)
