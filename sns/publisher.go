package sns

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type PublisherConfig struct {
	AWSConfig             aws.Config
	CreateTopicConfig     SNSConfigAtrributes
	CreateTopicfNotExists bool
	Marshaler             Marshaler
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter
	sns    *sns.Client
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()
	return &Publisher{
		sns:    sns.NewFromConfig(config.AWSConfig),
		config: config,
		logger: logger,
	}, nil
}

func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	ctx := context.Background()
	topicArn, err := p.GetArnTopic(ctx, topic)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		p.logger.Debug("Sending message", watermill.LogFields{"msg": msg})
		// Real messageId are generated on server side
		// so we can set our own here so we can use it in the tests
		// There is a deduplicationId but just for FIFO queues
		input := p.config.Marshaler.Marshal(msg)
		input.TopicArn = topicArn
		_, err = p.sns.Publish(ctx, input)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p Publisher) GetArnTopic(ctx context.Context, topic string) (*string, error) {
	topicARN, err := CheckARNTopic(ctx, p.sns, topic)
	if err != nil {
		if p.config.CreateTopicfNotExists {
			topicARN, err = CreateSNS(ctx, p.sns, topic, sns.CreateTopicInput{
				Attributes: p.config.CreateTopicConfig.Attributes(),
			})
			if err == nil {
				return topicARN, nil
			}
		}
		return nil, err
	}
	return topicARN, nil
}

func (p Publisher) Close() error {
	return nil
}

func (p Publisher) AddSubscription(ctx context.Context, config *sns.SubscribeInput) error {
	subcribeOutput, err := p.sns.Subscribe(ctx, config)
	if err != nil || subcribeOutput == nil {
		return fmt.Errorf("cannot subscribe to SNS[%s] from %s: %w", *config.TopicArn, *config.Endpoint, err)
	}
	return nil
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshalerUnmarshaler{}
	}
}
