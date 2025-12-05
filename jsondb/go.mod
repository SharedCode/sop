module github.com/sharedcode/sop/jsondb

go 1.24.3

replace github.com/sharedcode/sop => ../

replace github.com/sharedcode/sop/infs => ../infs

replace github.com/sharedcode/sop/adapters/redis => ../adapters/redis

replace github.com/sharedcode/sop/adapters/cassandra => ../adapters/cassandra

require (
	github.com/google/uuid v1.6.0
	github.com/sharedcode/sop v0.0.0
	github.com/sharedcode/sop/infs v0.0.0-00010101000000-000000000000
)

require (
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.12.4 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
)
