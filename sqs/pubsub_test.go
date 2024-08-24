package sqs_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPubSub(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false, // todo?
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPubSub_stress(t *testing.T) {
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) * 2)

	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false, // todo?
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_batching(t *testing.T) {
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
			},
		},
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			logger := watermill.NewStdLogger(false, false)

			cfg := newAwsConfig(t)

			pub, err := sqs.NewPublisher(sqs.PublisherConfig{
				AWSConfig: cfg,
				CreateQueueConfig: sqs.QueueConfigAttributes{
					// Default value is 30 seconds - need to be lower for tests
					VisibilityTimeout: "1",
				},
				CreateQueueIfNotExists: true,
				Marshaler:              sqs.DefaultMarshalerUnmarshaler{},
			}, logger)
			require.NoError(t, err)

			sub, err := sqs.NewSubscriber(sqs.SubscriberConfig{
				AWSConfig: cfg,
				QueueConfigAttributes: sqs.QueueConfigAttributes{
					// Default value is 30 seconds - need to be lower for tests
					VisibilityTimeout: "1",
				},
				GenerateReceiveMessageInput: func(ctx context.Context, queueURL string) (*awssqs.ReceiveMessageInput, error) {
					in, err := sqs.GenerateReceiveMessageInputDefault(ctx, queueURL)
					if err != nil {
						return nil, err
					}

					// this will effectively enable batching
					in.MaxNumberOfMessages = 10

					return in, nil
				},
				Unmarshaler: sqs.DefaultMarshalerUnmarshaler{},
			}, logger)
			require.NoError(t, err)

			return pub, sub
		},
	)
}

func TestPublishSubscribe_creating_queue_with_different_settings_should_be_idempotent(t *testing.T) {
	logger := watermill.NewStdLogger(false, false)

	sub1, err := sqs.NewSubscriber(sqs.SubscriberConfig{
		AWSConfig: newAwsConfig(t),
		QueueConfigAttributes: sqs.QueueConfigAttributes{
			// Default value is 30 seconds - need to be lower for tests
			VisibilityTimeout: "1",
		},
		Unmarshaler: sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	sub2, err := sqs.NewSubscriber(sqs.SubscriberConfig{
		AWSConfig: newAwsConfig(t),
		QueueConfigAttributes: sqs.QueueConfigAttributes{
			// Default value is 30 seconds - need to be lower for tests
			VisibilityTimeout: "20",
		},
		Unmarshaler: sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	topicName := watermill.NewUUID()

	require.NoError(t, sub1.SubscribeInitialize(topicName))
	require.NoError(t, sub2.SubscribeInitialize(topicName))
}

func TestPublisher_GetOrCreateQueueUrl_is_idempotent(t *testing.T) {
	pub, _ := createPubSub(t)

	topicName := watermill.NewUUID()

	url1, err := pub.(*sqs.Publisher).GetOrCreateQueueUrl(context.Background(), topicName)
	require.NoError(t, err)

	url2, err := pub.(*sqs.Publisher).GetOrCreateQueueUrl(context.Background(), topicName)
	require.NoError(t, err)

	require.Equal(t, url1, url2)
}

func TestSubscriber_doesnt_hang_when_queue_doesnt_exist(t *testing.T) {
	_, sub := createPubSub(t)
	msgs, err := sub.Subscribe(context.Background(), "non-existing-queue")

	require.ErrorContains(t, err, "cannot get queue for topic non-existing-queue: cannot get queue non-existing-queue")
	require.Nil(t, msgs)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(false, false)

	cfg := newAwsConfig(t)

	pub, err := sqs.NewPublisher(sqs.PublisherConfig{
		AWSConfig: cfg,
		CreateQueueConfig: sqs.QueueConfigAttributes{
			// Default value is 30 seconds - need to be lower for tests
			VisibilityTimeout: "1",
		},
		CreateQueueIfNotExists: true,
		Marshaler:              sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	sub, err := sqs.NewSubscriber(sqs.SubscriberConfig{
		AWSConfig: cfg,
		QueueConfigAttributes: sqs.QueueConfigAttributes{
			// Default value is 30 seconds - need to be lower for tests
			VisibilityTimeout: "1",
		},
		Unmarshaler: sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	return pub, sub
}

func newAwsConfig(t *testing.T) aws.Config {
	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("us-west-2"),
		awsconfig.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
			},
		}),
		connection.SetEndPoint("http://localhost:4566"),
	)
	require.NoError(t, err)
	return cfg
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return createPubSub(t)
}
