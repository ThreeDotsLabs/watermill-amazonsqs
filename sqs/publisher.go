package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
)

type PublisherConfig struct {
	AWSConfig aws.Config
	Marshaler Marshaler
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter
	sqs    *sqs.SQS
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.AWSConfig = connection.SetEndPoint(config.AWSConfig)
	sess, err := session.NewSession(&config.AWSConfig)
	if err != nil {
		// TODO wrap
		return nil, err
	}

	return &Publisher{
		sqs:    sqs.New(sess),
		config: config,
		logger: logger,
	}, nil
}

func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	// TODO method for generating
	queueName := topic

	result, err := p.sqs.GetQueueUrl(&sqs.GetQueueUrlInput{
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
		_, err = p.sqs.SendMessage(&sqs.SendMessageInput{
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
