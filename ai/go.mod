module github.com/sharedcode/sop/ai

go 1.24.3

require (
	github.com/google/cel-go v0.25.0
	github.com/google/uuid v1.6.0
	github.com/sethvargo/go-retry v0.3.0
	github.com/sharedcode/sop v1.2.0
	github.com/sharedcode/sop/infs v0.0.0
	github.com/sharedcode/sop/jsondb v0.0.0-20251228215734-8fde503e747f
	github.com/sharedcode/sop/search v0.0.0
	github.com/stretchr/testify v1.11.1
	github.com/yuin/goldmark v1.8.2
	golang.org/x/sync v0.19.0
)

require (
	cel.dev/expr v0.23.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.12.4 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.8.0 // indirect
	github.com/sharedcode/sop/adapters/redis v0.0.0-00010101000000-000000000000 // indirect
	github.com/sharedcode/sop/incfs v0.0.0-20251228215734-8fde503e747f // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	golang.org/x/exp v0.0.0-20230515195305-f3d0a9c9a5cc // indirect
	golang.org/x/sys v0.38.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/protobuf v1.36.9 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/sharedcode/sop => ../

replace github.com/sharedcode/sop/infs => ../infs

replace github.com/sharedcode/sop/adapters/redis => ../adapters/redis

replace github.com/sharedcode/sop/search => ../search
