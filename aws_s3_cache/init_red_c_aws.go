package aws_s3_cache

import (
	"fmt"

	"github.com/SharedCode/sop/aws_s3"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_ck"
	"github.com/SharedCode/sop/redis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var S3Client *s3.Client

// Assign the configs & open connections to different sub-systems used by this package.
// Example, connection to Cassandra, Redis.
func Initialize(cassandraConfig cas.Config, redisConfig redis.Options, awsConfig aws_s3.Config) error {
	S3Client = aws_s3.Connect(awsConfig)
	if S3Client == nil {
		return fmt.Errorf("can't connect to AWS S3")
	}
	return in_red_ck.Initialize(cassandraConfig, redisConfig)
}

// Returns true if components required were initialized, false otherwise.
func IsInitialized() bool {
	return in_red_ck.IsInitialized() && S3Client != nil
}

// Shutdown or closes all connections used in this package.
func Shutdown() {
	in_red_ck.Shutdown()
	S3Client = nil
}
