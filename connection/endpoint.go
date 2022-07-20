package connection

import (
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const AWS_ENDPOINT = "AWS_ENDPOINT"

func SetEndPoint(config aws.Config) aws.Config {
	newConfig := config
	awsEndpoint := os.Getenv(AWS_ENDPOINT)
	if awsEndpoint != "" {
		newConfig.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: awsEndpoint,
			}, nil
		})
	}
	return newConfig
}
