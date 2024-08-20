package sqs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SubscriberConfig struct {
	AWSConfig aws.Config

	// How long about unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration
	// Delete message attempts
	CreateQueueInitializerConfig QueueConfigAttributes

	GenerateCreateQueueInput GenerateCreateQueueInputFunc

	GenerateGetQueueUrlInput GenerateGetQueueUrlInputFunc

	GenerateReceiveMessageInput GenerateReceiveMessageInputFunc

	GenerateDeleteMessageInput GenerateDeleteMessageInputFunc

	Unmarshaler Unmarshaler
}

func (c *SubscriberConfig) SetDefaults() {
	if c.Unmarshaler == nil {
		c.Unmarshaler = DefaultMarshalerUnmarshaler{}
	}

	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}

	if c.GenerateCreateQueueInput == nil {
		c.GenerateCreateQueueInput = GenerateCreateQueueInputDefault
	}

	if c.GenerateGetQueueUrlInput == nil {
		c.GenerateGetQueueUrlInput = GenerateGetQueueUrlInputDefault
	}

	if c.GenerateReceiveMessageInput == nil {
		c.GenerateReceiveMessageInput = GenerateReceiveMessageInputDefault
	}

	if c.GenerateDeleteMessageInput == nil {
		c.GenerateDeleteMessageInput = GenerateDeleteMessageInputDefault
	}
}

func (c SubscriberConfig) Validate() error {
	var err error

	if c.AWSConfig.Credentials == nil {
		err = errors.Join(err, errors.New("missing Config.Credentials"))

	}
	if c.Unmarshaler == nil {
		err = errors.Join(err, errors.New("missing Config.Marshaler"))
	}

	return err
}

type PublisherConfig struct {
	AWSConfig aws.Config

	CreateQueueConfig      QueueConfigAttributes
	CreateQueueIfNotExists bool

	GenerateGetQueueUrlInput GenerateGetQueueUrlInputFunc

	GenerateSendMessageInput GenerateSendMessageInputFunc

	GenerateCreateQueueInput GenerateCreateQueueInputFunc

	Marshaler Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshalerUnmarshaler{}
	}

	if c.GenerateGetQueueUrlInput == nil {
		c.GenerateGetQueueUrlInput = GenerateGetQueueUrlInputDefault
	}

	if c.GenerateSendMessageInput == nil {
		c.GenerateSendMessageInput = GenerateSendMessageInputDefault
	}

	if c.GenerateCreateQueueInput == nil {
		c.GenerateCreateQueueInput = GenerateCreateQueueInputDefault
	}
}

type GenerateCreateQueueInputFunc func(ctx context.Context, topic string, attrs QueueConfigAttributes) (*sqs.CreateQueueInput, error)

func GenerateCreateQueueInputDefault(ctx context.Context, topic string, attrs QueueConfigAttributes) (*sqs.CreateQueueInput, error) {
	attrsMap, err := attrs.Attributes()
	if err != nil {
		return nil, fmt.Errorf("cannot generate attributes for queue %s: %w", topic, err)
	}

	return &sqs.CreateQueueInput{
		QueueName:  aws.String(topic),
		Attributes: attrsMap,
	}, nil
}

type GenerateGetQueueUrlInputFunc func(ctx context.Context, topic string) (*sqs.GetQueueUrlInput, error)

func GenerateGetQueueUrlInputDefault(ctx context.Context, topic string) (*sqs.GetQueueUrlInput, error) {
	return &sqs.GetQueueUrlInput{
		QueueName: aws.String(topic),
	}, nil
}

type GenerateReceiveMessageInputFunc func(ctx context.Context, queueURL string) (*sqs.ReceiveMessageInput, error)

func GenerateReceiveMessageInputDefault(ctx context.Context, queueURL string) (*sqs.ReceiveMessageInput, error) {
	return &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MessageAttributeNames: []string{"All"},
		WaitTimeSeconds:       30,
	}, nil
}

type GenerateDeleteMessageInputFunc func(ctx context.Context, queueURL string, receiptHandle *string) (*sqs.DeleteMessageInput, error)

func GenerateDeleteMessageInputDefault(ctx context.Context, queueURL string, receiptHandle *string) (*sqs.DeleteMessageInput, error) {
	return &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: receiptHandle,
	}, nil
}

type GenerateSendMessageInputFunc func(ctx context.Context, queueURL string, msg *types.Message) (*sqs.SendMessageInput, error)

func GenerateSendMessageInputDefault(ctx context.Context, queueURL string, msg *types.Message) (*sqs.SendMessageInput, error) {
	return &sqs.SendMessageInput{
		QueueUrl:          &queueURL,
		MessageAttributes: msg.MessageAttributes,
		MessageBody:       msg.Body,
	}, nil
}
