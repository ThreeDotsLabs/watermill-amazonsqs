package sns

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
)

func TestCreatePub(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	cfg := aws.Config{
		Region: "eu-north-1",
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: "http://localhost:9324",
			}, nil
		}),
	}

	_, err := NewPublisher(PublisherConfig{
		AWSConfig: cfg,
	}, logger)
	require.NoError(t, err)
}
