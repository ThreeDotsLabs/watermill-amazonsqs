package sqs

import (
	"context"
	"errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SubscriberConfig struct {
	AWSConfig   aws.Config
	Unmarshaler UnMarshaler
}

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter
	sqs    *sqs.Client
}

func NewSubsciber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.AWSConfig = connection.SetEndPoint(config.AWSConfig)
	return &Subscriber{
		config: config,
		logger: logger,
	}, nil
}

func (s Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	// TODO context cancel

	s.sqs = sqs.NewFromConfig(s.config.AWSConfig)

	output := make(chan *message.Message)

	queueURL, err := s.queueURL(topic)
	if err != nil {
		// TODO wrap
		return nil, err
	}

	s.logger.Trace("Listening for messages", nil)

	go func() {
		for {
			err := s.receive(ctx, queueURL, output)
			if err != nil {
				// TODO handle error
				panic(err)
			}
		}
	}()

	return output, nil
}

func (s Subscriber) receive(ctx context.Context, queueURL string, output chan *message.Message) error {
	result, err := s.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		WaitTimeSeconds: 1,
		QueueUrl:        aws.String(queueURL),
		MessageAttributeNames: []string{
			string(types.QueueAttributeNameAll),
		},
	})
	if err != nil {
		return err
	}

	for _, sqsMsg := range result.Messages {
		msg, err := s.config.Unmarshaler.Unmarshal(sqsMsg)
		if err != nil {
			return err
		}

		ctx, cancelCtx := context.WithCancel(ctx)
		msg.SetContext(ctx)
		// TODO
		defer cancelCtx()

		output <- msg

		select {
		case <-msg.Acked():
			err := s.deleteMessage(ctx, queueURL, sqsMsg.ReceiptHandle)
			if err != nil {
				// TODO handle
				return err
			}
		case <-msg.Nacked():
			// TODO Probably no action to be taken
		}
	}

	return nil
}

func (s Subscriber) deleteMessage(ctx context.Context, queueURL string, receiptHandle *string) error {
	_, err := s.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		// TODO wrap
		return err
	}

	return nil
}

func (s Subscriber) SubscribeInitialize(topic string) error {
	// TODO move
	s.sqs = sqs.NewFromConfig(s.config.AWSConfig)

	_, err := s.queueURL(topic)
	return err
}

func (s Subscriber) queueURL(topic string) (string, error) {
	// TODO add function mapping topic to queue name
	queueName := topic

	s.logger.Trace("Getting queue URL", nil)

	result, err := s.sqs.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err == nil {
		s.logger.Trace("Queue exists", nil)
		return *result.QueueUrl, nil
	}

	var queueDoesNotExist *types.QueueDoesNotExist

	if errors.As(err, &queueDoesNotExist) {
		s.logger.Trace("Creating queue", nil)
		createResult, err := s.sqs.CreateQueue(context.Background(), &sqs.CreateQueueInput{
			// TODO attributes from config
			QueueName: aws.String(queueName),
		})
		if err != nil {
			return "", err
		}

		return *createResult.QueueUrl, nil
	}

	return "", err
}

func (s Subscriber) Close() error {
	return nil
}
