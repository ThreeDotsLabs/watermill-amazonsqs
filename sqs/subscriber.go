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
		close(output)
		cancel()
		return nil, fmt.Errorf("cannot get queue for topic %s: %w", topic, err)
	}
	if queueURL == nil {
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

	s.logger.With(watermill.LogFields{"queue": queueURL}).Debug("Subscribing to queue", nil)

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

	go func() {
		<-s.closing
		cancelCtx()
	}()

	var sleepTime time.Duration = 0
	for {
		select {
		case <-s.closing:
			s.logger.Trace("Discarding queued message, subscriber closing", logFields)
			return

		case <-ctx.Done():
			s.logger.Trace("Stopping consume, context canceled", logFields)
			return

		case <-time.After(sleepTime):
			// Wait if needed
		}

		result, err := s.sqs.ReceiveMessage(ctx, input)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				sleepTime = NoSleep
				continue
			} else {
				s.logger.Error("Cannot connect receive messages", err, logFields)
				sleepTime = s.config.ReconnectRetrySleep
				continue
			}
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
	for _, sqsMsg := range messages {
		processed := s.processMessage(ctx, logFields, sqsMsg, output, queueURL)

		if !processed {
			return
		}
	}
}

func (s *Subscriber) processMessage(ctx context.Context, logFields watermill.LogFields, sqsMsg types.Message, output chan *message.Message, queueURL string) bool {
	s.logger.Trace("processMessage", logFields)

	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	msg, err := s.config.Unmarshaler.Unmarshal(&sqsMsg)
	if err != nil {
		s.logger.Error("Cannot unmarshal message", err, logFields)
		return false
	}
	msg.SetContext(ctx)
	output <- msg

	select {
	case <-msg.Acked():
		err := s.deleteMessage(ctx, queueURL, sqsMsg.ReceiptHandle, logFields)
		if errors.Is(err, context.Canceled) {
			return false
		}
		if err != nil {
			s.logger.Error("Failed to delete message", err, logFields)
			return false
		}
	case <-msg.Nacked():
		// Do not delete message, it will be redelivered
		return false // we don't want to process next messages to preserve order
	case <-s.closing:
		s.logger.Trace("Closing, message discarded before ack", logFields)
		return false
	case <-ctx.Done():
		s.logger.Trace("Closing, ctx cancelled before ack", logFields)
		return false
	}

	return true
}

func (s *Subscriber) deleteMessage(ctx context.Context, queueURL string, receiptHandle *string, logFields watermill.LogFields) error {
	input, err := s.config.GenerateDeleteMessageInput(ctx, queueURL, receiptHandle)
	if err != nil {
		return fmt.Errorf("cannot generate input for delete message: %w", err)
	}

	_, err = s.sqs.DeleteMessage(ctx, input)
	if err != nil {
		var oe *smithy.GenericAPIError
		if errors.As(err, &oe) {
			if oe.Message == "The specified queue does not contain the message specified." {
				s.logger.Trace("Message was already deleted or is not in queue", logFields)
				return nil
			}
		}
		return fmt.Errorf("cannot ack (delete) message: %w", err)
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
