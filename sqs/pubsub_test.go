package sqs_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_stress(t *testing.T) {
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) * 2)

	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(false, false)

	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("us-west-2"),
		awsconfig.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
			},
		}),
		connection.SetEndPoint("http://localhost:4100"),
	)
	require.NoError(t, err)

	pub, err := sqs.NewPublisher(sqs.PublisherConfig{
		AWSConfig: cfg,
		CreateQueueConfig: sqs.QueueConfigAttributes{
			// Defalt value is 30 seconds - need to be lower for tests
			VisibilityTimeout: "1",
		},
		CreateQueueIfNotExists: true,
		Marshaler:              sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	sub, err := sqs.NewSubscriber(sqs.SubscriberConfig{
		AWSConfig: cfg,
		CreateQueueInitializerConfig: sqs.QueueConfigAttributes{
			// Defalt value is 30 seconds - need to be lower for tests
			VisibilityTimeout: "1",
		},
		Unmarshaler: sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	return pub, sub
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return createPubSub(t)
}
