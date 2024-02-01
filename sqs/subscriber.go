package sqs

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/hashicorp/go-multierror"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type SubscriberConfig struct {
	AWSConfig aws.Config
	// How long about unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration
	// Delete message attemps
	CreateQueueInitializerConfig QueueConfigAtrributes

	Unmarshaler UnMarshaler
}

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter
	sqs    *sqs.Client

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed bool
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": watermill.NewShortUUID(),
	})

	return &Subscriber{
		config:  config,
		logger:  logger,
		sqs:     sqs.NewFromConfig(config.AWSConfig),
		closing: make(chan struct{}),
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	ctx, cancel := context.WithCancel(ctx)
	output := make(chan *message.Message)

	queueURL, err := GetQueueUrl(ctx, s.sqs, topic)
	if err != nil {
		close(output)
		cancel()
		return nil, err
	}

	s.subscribersWg.Add(1)

	go func() {
		s.receive(ctx, *queueURL, output)
		close(output)
		cancel()
	}()

	return output, nil
}

func (s *Subscriber) receive(ctx context.Context, queueURL string, output chan *message.Message) {
	defer s.subscribersWg.Done()
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
	logFields := watermill.LogFields{
		"provider": "aws",
		"queue":    queueURL,
	}

	var sleepTime time.Duration = 0
	for {
		select {
		case <-s.closing:
			s.logger.Info("Discarding queued message, subscriber closing", logFields)
			return

		case <-ctx.Done():
			s.logger.Info("Stopping consume, context canceled", logFields)
			return

		case <-time.After(sleepTime): // Wait if needed
		}

		result, err := s.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(queueURL),
			MessageAttributeNames: []string{"All"},
		})

		if err != nil {
			s.logger.Error("Cannot connect recieve messages", err, logFields)
			sleepTime = s.config.ReconnectRetrySleep
			continue
		}

		sleepTime = NoSleep
		if result == nil || len(result.Messages) == 0 {
			continue
		}
		s.ConsumeMessages(ctx, result.Messages, queueURL, output, logFields)
	}
}

func (s *Subscriber) ConsumeMessages(
	ctx context.Context,
	messages []types.Message,
	queueURL string,
	output chan *message.Message,
	logFields watermill.LogFields,
) {

	for _, sqsMsg := range messages {
		ctx, cancelCtx := context.WithCancel(ctx)
		defer cancelCtx()
		msg, err := s.config.Unmarshaler.Unmarshal(&sqsMsg)
		if err != nil {
			s.logger.Error("Cannot unmarshal message", err, logFields)
			return
		}
		msg.SetContext(ctx)
		output <- msg

		select {
		case <-msg.Acked():
			err := s.deleteMessage(ctx, queueURL, sqsMsg.ReceiptHandle)
			if err != nil {
				s.logger.Error("Failed to delete message", err, logFields)
				return
			}
		case <-msg.Nacked():
			// Do not delete message, it will be redelivered
		case <-s.closing:
			s.logger.Trace("Closing, message discarded before ack", logFields)
			return
		case <-ctx.Done():
			s.logger.Trace("Closing, ctx cancelled before ack", logFields)
			return
		}
	}
}

func (s *Subscriber) deleteMessage(ctx context.Context, queueURL string, receiptHandle *string) error {
	_, err := s.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: receiptHandle,
	})

	if err != nil {
		var oe *smithy.GenericAPIError
		if errors.As(err, &oe) {
			if oe.Message == "The specified queue does not contain the message specified." {
				// Message was already deleted or is not in queue
				return nil
			}
		}
		return err
	}

	return nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribersWg.Wait()

	return nil
}

func (s *Subscriber) SubscribeInitialize(topic string) error {
	_, err := CreateQueue(context.Background(), s.sqs, topic, sqs.CreateQueueInput{
		Attributes: s.config.CreateQueueInitializerConfig.Attributes(),
	})
	return err
}

func (s *Subscriber) GetQueueUrl(ctx context.Context, topic string) (*string, error) {
	queueUrl, err := GetQueueUrl(ctx, s.sqs, topic)
	return queueUrl, err
}

func (s *Subscriber) GetQueueArn(ctx context.Context, url *string) (*string, error) {
	return GetARNUrl(ctx, s.sqs, url)
}

const NoSleep time.Duration = -1

func (c *SubscriberConfig) SetDefaults() {

	if c.Unmarshaler == nil {
		c.Unmarshaler = DefaultMarshalerUnmarshaler{}
	}

	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
}
func (c SubscriberConfig) Validate() error {
	var err error

	if c.AWSConfig.Credentials == nil {
		err = multierror.Append(err, errors.New("missing Config.Credentials"))

	}
	if c.Unmarshaler == nil {
		err = multierror.Append(err, errors.New("missing Config.Marshaler"))
	}

	return err
}
