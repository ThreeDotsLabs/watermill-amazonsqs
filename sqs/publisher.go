package sqs

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

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

	// todo: cache it
	queueUrl, err := p.GetQueueUrl(ctx, topic)
	if err != nil {
		return fmt.Errorf("cannot get queue url: %w", err)
	}
	if queueUrl == nil {
		return fmt.Errorf("returned queueUrl is nil")
	}

	for _, msg := range messages {
		sqsMsg, err := p.config.Marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("cannot marshal message: %w", err)
		}

		p.logger.Debug("Sending message", watermill.LogFields{"msg": msg})

		input, err := p.config.GenerateSendMessageInput(ctx, *queueUrl, sqsMsg)
		if err != nil {
			return fmt.Errorf("cannot generate send message input: %w", err)
		}

		_, err = p.sqs.SendMessage(ctx, input)
		if err != nil {
			return fmt.Errorf("cannot send message: %w", err)
		}
	}

	return nil
}

func (p Publisher) GetQueueUrl(ctx context.Context, topic string) (*string, error) {
	queueUrl, err := getQueueUrl(ctx, p.sqs, topic, p.config.GenerateGetQueueUrlInput)
	if err != nil {
		// todo: check exact error here
		if p.config.CreateQueueIfNotExists {
			input, err := p.config.GenerateCreateQueueInput(ctx, topic, p.config.CreateQueueConfig)
			if err != nil {
				return nil, fmt.Errorf("cannot generate create queue input: %w", err)
			}

			queueUrl, err = greateQueue(ctx, p.sqs, input)
			if err == nil {
				return queueUrl, nil
			}
		}
		return nil, err
	}
	return queueUrl, nil
}

func (p Publisher) GetQueueArn(ctx context.Context, url *string) (*string, error) {
	return getARNUrl(ctx, p.sqs, url)
}

func (p Publisher) Close() error {
	return nil
}

// todo: missing validate?
