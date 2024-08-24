package sns_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sns"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
			GenerateTopicFunc: func(tctx tests.TestContext) string {
				return fmt.Sprintf("arn:aws:sns:us-west-2:000000000000:%s", tctx.TestID)
			},
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPublisher_CreateTopic_is_idempotent(t *testing.T) {
	pub, _ := createPubSub(t)

	topicName := "arn:aws:sns:us-west-2:000000000000:" + watermill.NewUUID()

	arn1, err := pub.(*sns.Publisher).CreateTopic(context.Background(), topicName)
	require.NoError(t, err)

	arn2, err := pub.(*sns.Publisher).CreateTopic(context.Background(), topicName)
	require.NoError(t, err)

	assert.Equal(t, arn1, arn2)
}

func TestSubscriber_SubscribeInitialize_is_idempotent(t *testing.T) {
	_, sub := createPubSub(t)

	topicName := "arn:aws:sns:us-west-2:000000000000:" + watermill.NewUUID()

	err := sub.(*sns.Subscriber).SubscribeInitialize(topicName)
	require.NoError(t, err)

	err = sub.(*sns.Subscriber).SubscribeInitialize(topicName)
	require.NoError(t, err)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)
	cfg, err := GetAWSConfig()
	require.NoError(t, err)

	pub, err := sns.NewPublisher(sns.PublisherConfig{
		AWSConfig:         cfg,
		CreateTopicConfig: sns.ConfigAttributes{
			// FifoTopic: "true",
		},
		Marshaler: sns.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	sub, err := sns.NewSubscriber(
		sns.SubscriberConfig{
			AWSConfig:            cfg,
			GenerateSqsQueueName: sns.GenerateSqsQueueNameEqualToTopicName,
		},
		sqs.SubscriberConfig{
			AWSConfig: cfg,
			QueueConfigAttributes: sqs.QueueConfigAttributes{
				// Default value is 30 seconds - need to be lower for tests
				VisibilityTimeout: "1",
			},
		},
		logger,
	)
	require.NoError(t, err)

	return pub, sub
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, false)
	cfg, err := GetAWSConfig()
	require.NoError(t, err)

	pub, err := sns.NewPublisher(sns.PublisherConfig{
		AWSConfig:         cfg,
		CreateTopicConfig: sns.ConfigAttributes{
			// FifoTopic: "true",
		},
		Marshaler: sns.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	sub, err := sns.NewSubscriber(
		sns.SubscriberConfig{
			AWSConfig: cfg,
			GenerateSqsQueueName: func(ctx context.Context, sqsTopic string) (string, error) {
				return consumerGroup, nil
			},
		},
		sqs.SubscriberConfig{
			AWSConfig: cfg,
			QueueConfigAttributes: sqs.QueueConfigAttributes{
				// Default value is 30 seconds - need to be lower for tests
				VisibilityTimeout: "1",
			},
		},
		logger,
	)
	require.NoError(t, err)

	return pub, sub
}

func GetAWSConfig() (aws.Config, error) {
	return awsconfig.LoadDefaultConfig(
		context.Background(),
		connection.SetEndPoint("http://localhost:4566"),
		awsconfig.WithRegion("us-west-2"),
	)
}
