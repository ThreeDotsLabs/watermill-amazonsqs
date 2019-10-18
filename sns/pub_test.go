package sns

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
)

func TestCreatePub(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	cfg := aws.Config{
		Region:   aws.String("eu-north-1"),
		Endpoint: aws.String("http://localhost:9324"),
	}

	_, err := NewPublisher(PublisherConfig{
		AWSConfig: cfg,
	}, logger)
	require.NoError(t, err)
}
