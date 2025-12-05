module github.com/sharedcode/sop/ai

go 1.24.3

require (
	github.com/sharedcode/sop v1.2.0
	github.com/sharedcode/sop/infs v0.0.0
	github.com/sharedcode/sop/search v0.0.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/klauspost/reedsolomon v1.12.4 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
)

replace github.com/sharedcode/sop => ../

replace github.com/sharedcode/sop/infs => ../infs

replace github.com/sharedcode/sop/adapters/redis => ../adapters/redis

replace github.com/sharedcode/sop/search => ../search
