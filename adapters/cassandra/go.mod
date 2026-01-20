module github.com/sharedcode/sop/adapters/cassandra

go 1.24.3

replace github.com/sharedcode/sop => ../../

require (
	github.com/gocql/gocql v1.7.0
	github.com/sethvargo/go-retry v0.3.0
	github.com/sharedcode/sop v0.0.0-00010101000000-000000000000
)

require (
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	golang.org/x/sync v0.19.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)
