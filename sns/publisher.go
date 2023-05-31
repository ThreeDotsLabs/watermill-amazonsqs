package sns

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type PublisherConfig struct {
	AWSConfig             aws.Config
	CreateTopicConfig     SNSConfigAtrributes
	CreateTopicfNotExists bool
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter
	sns    *sns.Client
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
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
		attributes := metadataToAttributes(msg.Metadata)
		attributes["UUID"] = types.MessageAttributeValue{
			StringValue: aws.String(msg.UUID),
			DataType:    aws.String("String"),
		}
		_, err = p.sns.Publish(ctx, &sns.PublishInput{
			TopicArn:          topicArn,
			Message:           aws.String(string(msg.Payload)),
			MessageAttributes: attributes,
		})
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

func metadataToAttributes(meta message.Metadata) map[string]types.MessageAttributeValue {
	attributes := make(map[string]types.MessageAttributeValue)

	for k, v := range meta {
		attributes[k] = types.MessageAttributeValue{
			StringValue: aws.String(v),
			DataType:    aws.String("String"),
		}
	}

	return attributes
}
