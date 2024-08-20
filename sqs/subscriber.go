package sqs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
)

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

	s.logger.With(watermill.LogFields{"topic": topic}).Trace("Getting queue", nil)

	ctx, cancel := context.WithCancel(ctx)
	output := make(chan *message.Message)

	queueURL, err := getQueueUrl(ctx, s.sqs, topic, s.config.GenerateGetQueueUrlInput)
	if err != nil {
		// todo: should be logged later
		// todo: better err handling
		s.logger.With(watermill.LogFields{
			"queue": queueURL,
			"topic": topic,
		}).Error("Failed to get queue", err, nil)

		close(output)
		cancel()
		return nil, err
	}
	if queueURL == nil {
		s.logger.With(watermill.LogFields{"topic": topic}).Trace("Queue not found", nil)
		close(output)
		cancel()
		return nil, fmt.Errorf("queue %s not found", topic)
	}

	receiveInput, err := s.config.GenerateReceiveMessageInput(ctx, *queueURL)
	if err != nil {
		close(output)
		cancel()
		return nil, fmt.Errorf("cannot generate input for queue %s: %w", topic, err)
	}

	s.logger.With(watermill.LogFields{"queue": queueURL}).Trace("Subscribing to queue", nil)

	s.subscribersWg.Add(1)

	go func() {
		s.receive(ctx, *queueURL, output, receiveInput)
		close(output)
		cancel()
	}()

	return output, nil
}

func (s *Subscriber) receive(ctx context.Context, queueURL string, output chan *message.Message, input *sqs.ReceiveMessageInput) {
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
			s.logger.Trace("Discarding queued message, subscriber closing", logFields)
			return

		case <-ctx.Done():
			s.logger.Trace("Stopping consume, context canceled", logFields)
			return

		case <-time.After(sleepTime): // Wait if needed
			s.logger.Trace("Timeout?", logFields)
		}

		result, err := s.sqs.ReceiveMessage(ctx, input)

		if err != nil {
			s.logger.Error("Cannot connect receive messages", err, logFields)
			sleepTime = s.config.ReconnectRetrySleep
			continue
		}

		sleepTime = NoSleep
		if result == nil || len(result.Messages) == 0 {
			s.logger.Trace("No messages", logFields)
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
	s.logger.Trace("ConsumeMessages", logFields)

	for _, sqsMsg := range messages {
		s.logger.Trace("ConsumeMessages", logFields)

		ctx, cancelCtx := context.WithCancel(ctx)
		defer cancelCtx() // todo: leak
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
	input, err := s.config.GenerateDeleteMessageInput(ctx, queueURL, receiptHandle)
	if err != nil {
		return fmt.Errorf("cannot generate input for delete message: %w", err)
	}

	_, err = s.sqs.DeleteMessage(ctx, input)

	if err != nil {
		var oe *smithy.GenericAPIError
		if errors.As(err, &oe) {
			if oe.Message == "The specified queue does not contain the message specified." {
				// Message was already deleted or is not in queue
				// todo: log?
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
	return s.SubscribeInitializeWithContext(context.Background(), topic)
}

func (s *Subscriber) SubscribeInitializeWithContext(ctx context.Context, topic string) error {
	input, err := s.config.GenerateCreateQueueInput(ctx, topic, s.config.CreateQueueInitializerConfig)
	if err != nil {
		return fmt.Errorf("cannot generate input for queue %s: %w", topic, err)
	}

	_, err = greateQueue(ctx, s.sqs, input)
	if err != nil {
		return fmt.Errorf("cannot create queue %s: %w", topic, err)
	}

	return nil
}

func (s *Subscriber) GetQueueUrl(ctx context.Context, topic string) (*string, error) {
	queueUrl, err := getQueueUrl(ctx, s.sqs, topic, s.config.GenerateGetQueueUrlInput)
	return queueUrl, err
}

func (s *Subscriber) GetQueueArn(ctx context.Context, url *string) (*string, error) {
	return getARNUrl(ctx, s.sqs, url)
}

const NoSleep time.Duration = -1
