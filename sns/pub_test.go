package sns_test

import (
	"context"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sns"
)

func TestCreatePub(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("us-west-2"),
		awsconfig.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
			},
		}),
		connection.SetEndPoint("http://localhost:9324"),
	)

	require.NoError(t, err)

	_, err = sns.NewPublisher(sns.PublisherConfig{
		AWSConfig: cfg,
	}, logger)

	require.NoError(t, err)
}
