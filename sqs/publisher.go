package sqs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type PublisherConfig struct {
	AWSConfig              aws.Config
	CreateQueueConfig      QueueConfigAtrributes
	CreateQueueIfNotExists bool
	Marshaler              Marshaler
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter
	sqs    *sqs.Client
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()
	return &Publisher{
		sqs:    sqs.NewFromConfig(config.AWSConfig),
		config: config,
		logger: logger,
	}, nil
}

func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	ctx := context.Background()
	queueUrl, err := p.GetQueueUrl(ctx, topic)
	if err != nil {
		return err
	}
	for _, msg := range messages {
		sqsMsg, err := p.config.Marshaler.Marshal(msg)
		if err != nil {
			return err
		}

		p.logger.Debug("Sending message", watermill.LogFields{"msg": msg})
		_, err = p.sqs.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:          queueUrl,
			MessageAttributes: sqsMsg.MessageAttributes,
			MessageBody:       sqsMsg.Body,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (p Publisher) GetQueueUrl(ctx context.Context, topic string) (*string, error) {
	queueUrl, err := GetQueueUrl(ctx, p.sqs, topic)
	if err != nil {
		if p.config.CreateQueueIfNotExists {
			queueUrl, err = CreateQueue(ctx, p.sqs, topic, sqs.CreateQueueInput{
				Attributes: p.config.CreateQueueConfig.Attributes(),
			})
			if err == nil {
				return queueUrl, nil
			}
		}
		return nil, err
	}
	return queueUrl, nil
}

func (p Publisher) GetQueueArn(ctx context.Context, url *string) (*string, error) {
	return GetARNUrl(ctx, p.sqs, url)
}

func (p Publisher) Close() error {
	return nil
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshalerUnmarshaler{}
	}
}
