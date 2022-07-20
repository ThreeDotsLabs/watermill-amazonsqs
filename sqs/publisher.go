package sqs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
)

type PublisherConfig struct {
	AWSConfig aws.Config
	Marshaler Marshaler
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter
	sqs    *sqs.Client
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.AWSConfig = connection.SetEndPoint(config.AWSConfig)

	return &Publisher{
		sqs:    sqs.NewFromConfig(config.AWSConfig),
		config: config,
		logger: logger,
	}, nil
}

func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	// TODO method for generating
	queueName := topic

	result, err := p.sqs.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return err
	}

	for _, msg := range messages {
		sqsMsg, err := p.config.Marshaler.Marshal(msg)
		if err != nil {
			return err
		}

		p.logger.Debug("Sending message", watermill.LogFields{"msg": msg})
		_, err = p.sqs.SendMessage(context.Background(), &sqs.SendMessageInput{
			QueueUrl:          result.QueueUrl,
			MessageAttributes: sqsMsg.MessageAttributes,
			MessageBody:       sqsMsg.Body,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (p Publisher) Close() error {
	return nil
}
