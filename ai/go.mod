module github.com/sharedcode/sop/ai

go 1.24.3

require (
	github.com/google/uuid v1.6.0
	github.com/sharedcode/sop v1.2.0
	github.com/sharedcode/sop/infs v0.0.0
	github.com/sharedcode/sop/jsondb v0.0.0-20251228215734-8fde503e747f
	github.com/sharedcode/sop/search v0.0.0
	github.com/stretchr/testify v1.11.1
	golang.org/x/sync v0.19.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gocql/gocql v1.7.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.12.4 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/redis/go-redis/v9 v9.8.0 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	github.com/sharedcode/sop/adapters/cassandra v0.0.0-20251228215734-8fde503e747f // indirect
	github.com/sharedcode/sop/adapters/redis v0.0.0-00010101000000-000000000000 // indirect
	github.com/sharedcode/sop/incfs v0.0.0-20251228215734-8fde503e747f // indirect
	golang.org/x/sys v0.38.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/sharedcode/sop => ../

replace github.com/sharedcode/sop/infs => ../infs

replace github.com/sharedcode/sop/adapters/redis => ../adapters/redis

replace github.com/sharedcode/sop/search => ../search
