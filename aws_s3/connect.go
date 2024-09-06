package aws_s3

import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	// "http://127.0.0.1:9000"
	HostEndpointUrl string
	// "us-east-1"
	Region string
	Username string
	Password string
}

// Connect to minio Server endpoint.
func Connect(config Config) *s3.Client {
	client := s3.NewFromConfig(aws.Config{Region: config.Region}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(config.HostEndpointUrl)
		o.Credentials = credentials.NewStaticCredentialsProvider(config.Username, config.Password, "")
	})
	return client
}
