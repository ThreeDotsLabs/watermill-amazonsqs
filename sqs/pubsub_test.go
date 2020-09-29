package sqs

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
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

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	cfg := aws.Config{
		Region:   aws.String("eu-north-1"),
		Endpoint: aws.String("http://localhost:4100"),
	}

	pub, err := NewPublisher(PublisherConfig{
		AWSConfig: cfg,
		Marshaler: DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	sub, err := NewSubsciber(SubscriberConfig{
		AWSConfig:   cfg,
		Unmarshaler: DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	return pub, sub
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return createPubSub(t)
}
