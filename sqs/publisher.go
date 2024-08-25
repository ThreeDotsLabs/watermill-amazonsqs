package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pkg/errors"
)

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter
	sqs    *sqs.Client
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &Publisher{
		sqs:    sqs.NewFromConfig(config.AWSConfig),
		config: config,
		logger: logger,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	ctx := context.Background()

	queueName, queueUrl, err := p.GetOrCreateQueueUrl(ctx, topic)
	if err != nil {
		return fmt.Errorf("cannot get queue url: %w", err)
	}

	for _, msg := range messages {
		sqsMsg, err := p.config.Marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("cannot marshal message: %w", err)
		}

		p.logger.Debug("Sending message", watermill.LogFields{"msg": msg})

		input, err := p.config.GenerateSendMessageInput(ctx, queueUrl, sqsMsg)
		if err != nil {
			return fmt.Errorf("cannot generate send message input: %w", err)
		}

		_, err = p.sqs.SendMessage(ctx, input)
		var queueDoesNotExistErr *types.QueueDoesNotExist
		if errors.As(err, &queueDoesNotExistErr) && !p.config.DoNotCreateQueueIfNotExists {
			// GetOrCreateQueueUrl may not create queue if QueueUrlResolver doesn't check if queue exists
			_, err := p.createQueue(ctx, topic, queueName, &queueUrl)
			if err != nil {
				return err
			}
		}
		if err != nil {
			return fmt.Errorf("cannot send message: %w", err)
		}
	}

	return nil
}

// todo: add types for queueName, etc. ? as it becomes messy what is what

// todo: name is stupid as creation is conditional
func (p *Publisher) GetOrCreateQueueUrl(ctx context.Context, topic string) (queueName string, queueURL string, err error) {
	queueName, queueUrl, exists, err := p.config.QueueUrlResolver.ResolveQueueUrl(ctx, ResolveQueueUrlParams{
		Topic:     topic,
		SqsClient: p.sqs,
		Logger:    p.logger,
	})
	if err != nil {
		return "", "", err
	}
	if exists {
		return queueName, *queueUrl, nil
	}

	if !p.config.DoNotCreateQueueIfNotExists {
		queueUrl, err := p.createQueue(ctx, topic, queueName, queueUrl)
		if err != nil {
			return "", "", err
		}

		return queueName, queueUrl, nil

	} else {
		return "", "", fmt.Errorf("queue for topic %s doesn't exist", topic)
	}
}

func (p *Publisher) createQueue(ctx context.Context, topic string, queueName string, queueUrl *string) (string, error) {
	input, err := p.config.GenerateCreateQueueInput(ctx, queueName, p.config.CreateQueueConfig)
	if err != nil {
		return "", fmt.Errorf("cannot generate create queue input: %w", err)
	}

	queueUrl, err = createQueue(ctx, p.sqs, input)
	if err != nil {
		return "", fmt.Errorf("cannot create queue: %w", err)
	}
	// queue was created in the meantime
	// todo: it's quite ugly
	if queueUrl == nil {
		_, queueUrl, exists, err := p.config.QueueUrlResolver.ResolveQueueUrl(ctx, ResolveQueueUrlParams{
			Topic:     topic,
			SqsClient: p.sqs,
			Logger:    p.logger,
		})
		if err != nil {
			return "", err
		}
		if !exists {
			return "", fmt.Errorf("queue doesn't exist after creation")
		}

		return *queueUrl, nil
	}

	return *queueUrl, nil
}

// todo: move (together with other funcs) to url.go
func generateGetQueueUrlInputHash(getQueueInput *sqs.GetQueueUrlInput) string {
	// we are not using fmt.Sprintf because of pointers under the hood
	// we are not hashing specific struct fields to keep forward compatibility
	// also, json.Marshal is faster than fmt.Sprintf
	b, _ := json.Marshal(getQueueInput)
	return string(b)
}

func (p *Publisher) GetQueueArn(ctx context.Context, url *string) (*string, error) {
	return getARNUrl(ctx, p.sqs, url)
}

func (p *Publisher) Close() error {
	return nil
}
