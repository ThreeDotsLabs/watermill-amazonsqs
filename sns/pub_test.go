package sns_test

import (
	"context"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	snsaws "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sns"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

type TestSNS struct {
	sns.Publisher
	sub *sqs.Subscriber
}

func (t *TestSNS) Publish(topic string, messages ...*message.Message) error {
	ctx := context.Background()
	pubArn, err := t.Publisher.GetArnTopic(ctx, topic)
	if err != nil {
		return err
	}
	sqsUrl, err := t.sub.GetQueueUrl(ctx, topic)
	if err != nil {
		return err
	}

	err = t.Publisher.AddSubscription(context.Background(), &snsaws.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: pubArn,
		Endpoint: sqsUrl,
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	})
	if err != nil {
		return err
	}
	err = t.Publisher.Publish(topic, messages...)
	return err
}

func TestCreatePub(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	cfg, err := GetAWSConfig()
	require.NoError(t, err)

	_, err = sns.NewPublisher(sns.PublisherConfig{
		AWSConfig: cfg,
	}, logger)

	require.NoError(t, err)
}

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
	logger := watermill.NewStdLogger(false, false)
	cfg, err := GetAWSConfig()
	require.NoError(t, err)

	pub, err := sns.NewPublisher(sns.PublisherConfig{
		AWSConfig:         cfg,
		CreateTopicConfig: sns.SNSConfigAtrributes{
			// FifoTopic: "true",
		},
		CreateTopicfNotExists: true,
		Marshaler:             sns.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)

	sub, err := sqs.NewSubscriber(sqs.SubscriberConfig{
		AWSConfig: cfg,
		CreateQueueInitializerConfig: sqs.QueueConfigAtrributes{
			// Defalt value is 30 seconds - need to be lower for tests
			VisibilityTimeout: "15",
		},
		Unmarshaler: sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	require.NoError(t, err)
	test := &TestSNS{
		Publisher: *pub,
		sub:       sub,
	}
	return test, sub
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return createPubSub(t)
}
func GetAWSConfig() (aws.Config, error) {
	return awsconfig.LoadDefaultConfig(
		context.Background(),
		connection.SetEndPoint("http://localhost:4100"),
	)
}
