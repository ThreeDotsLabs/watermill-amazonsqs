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
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPubSub_arn_topic_resolver(t *testing.T) {
	tests.TestPublishSubscribe(
		t,
		tests.TestContext{
			TestID: tests.NewTestID(),
			Features: tests.Features{
				ConsumerGroups:                      true,
				ExactlyOnceDelivery:                 false,
				GuaranteedOrder:                     true,
				GuaranteedOrderWithSingleSubscriber: true,
				Persistent:                          true,
				GenerateTopicFunc: func(tctx tests.TestContext) string {
					return fmt.Sprintf("arn:aws:sns:us-west-2:000000000000:%s", tctx.TestID)
				},
			},
		},
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			cfg := GetAWSConfig(t)

			return createPubSubWithConfig(
				t,
				sns.PublisherConfig{
					AWSConfig:         cfg,
					CreateTopicConfig: sns.ConfigAttributes{},
					Marshaler:         sns.DefaultMarshalerUnmarshaler{},
					TopicResolver:     sns.TransparentTopicResolver{},
				},
				sns.SubscriberConfig{
					AWSConfig:            cfg,
					GenerateSqsQueueName: sns.GenerateSqsQueueNameEqualToTopicName,
					TopicResolver:        sns.TransparentTopicResolver{},
				},
				sqs.SubscriberConfig{
					AWSConfig: cfg,
					QueueConfigAttributes: sqs.QueueConfigAttributes{
						// Default value is 30 seconds - need to be lower for tests
						VisibilityTimeout: "1",
					},
				},
			)
		},
	)
}

func TestPublisher_CreateTopic_is_idempotent(t *testing.T) {
	pub, _ := createPubSub(t)

	topicName := watermill.NewUUID()

	arn1, err := pub.(*sns.Publisher).CreateTopic(context.Background(), topicName)
	require.NoError(t, err)

	arn2, err := pub.(*sns.Publisher).CreateTopic(context.Background(), topicName)
	require.NoError(t, err)

	assert.Equal(t, arn1, arn2)
}

func TestSubscriber_SubscribeInitialize_is_idempotent(t *testing.T) {
	_, sub := createPubSub(t)

	topicName := watermill.NewUUID()

	err := sub.(*sns.Subscriber).SubscribeInitialize(topicName)
	require.NoError(t, err)

	err = sub.(*sns.Subscriber).SubscribeInitialize(topicName)
	require.NoError(t, err)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	cfg := GetAWSConfig(t)

	topicResolver, err := sns.NewGenerateArnTopicResolver("000000000000", "us-west-2")
	require.NoError(t, err)

	return createPubSubWithConfig(
		t,
		sns.PublisherConfig{
			AWSConfig:         cfg,
			CreateTopicConfig: sns.ConfigAttributes{},
			TopicResolver:     topicResolver,
			Marshaler:         sns.DefaultMarshalerUnmarshaler{},
		},
		sns.SubscriberConfig{
			AWSConfig:            cfg,
			TopicResolver:        topicResolver,
			GenerateSqsQueueName: sns.GenerateSqsQueueNameEqualToTopicName,
		},
		sqs.SubscriberConfig{
			AWSConfig: cfg,
			QueueConfigAttributes: sqs.QueueConfigAttributes{
				// Default value is 30 seconds - need to be lower for tests
				VisibilityTimeout: "1",
			},
		},
	)
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	cfg := GetAWSConfig(t)

	topicResolver, err := sns.NewGenerateArnTopicResolver("000000000000", "us-west-2")
	require.NoError(t, err)

	return createPubSubWithConfig(
		t,
		sns.PublisherConfig{
			AWSConfig:         cfg,
			CreateTopicConfig: sns.ConfigAttributes{},
			Marshaler:         sns.DefaultMarshalerUnmarshaler{},
			TopicResolver:     topicResolver,
		},
		sns.SubscriberConfig{
			AWSConfig: cfg,
			GenerateSqsQueueName: func(ctx context.Context, sqsTopic string) (string, error) {
				return consumerGroup, nil
			},
			TopicResolver: topicResolver,
		},
		sqs.SubscriberConfig{
			AWSConfig: cfg,
			QueueConfigAttributes: sqs.QueueConfigAttributes{
				// Default value is 30 seconds - need to be lower for tests
				VisibilityTimeout: "1",
			},
		},
	)
}

func createPubSubWithConfig(
	t *testing.T,
	pubConfig sns.PublisherConfig,
	subConfig sns.SubscriberConfig,
	sqsSubConfig sqs.SubscriberConfig,
) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, false)

	pub, err := sns.NewPublisher(pubConfig, logger)
	require.NoError(t, err)

	sub, err := sns.NewSubscriber(subConfig, sqsSubConfig, logger)
	require.NoError(t, err)

	return pub, sub
}

func GetAWSConfig(t *testing.T) aws.Config {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		connection.SetEndPoint("http://localhost:4566"),
		awsconfig.WithRegion("us-west-2"),
	)
	require.NoError(t, err)

	return cfg
}