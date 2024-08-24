package sns

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter

	sns *sns.Client
	sqs *sqs.Subscriber
}

func NewSubscriber(
	config SubscriberConfig,
	sqsConfig sqs.SubscriberConfig,
	logger watermill.LoggerAdapter,
) (*Subscriber, error) {
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

	sqs, err := sqs.NewSubscriber(sqsConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("cannot create SQS subscriber: %w", err)
	}

	return &Subscriber{
		config: config,
		logger: logger,
		sns:    sns.NewFromConfig(config.AWSConfig),
		sqs:    sqs,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, snsTopicArn string) (<-chan *message.Message, error) {
	if !s.config.DoNotSubscribeToSns {
		if err := s.SubscribeInitializeWithContext(ctx, snsTopicArn); err != nil {
			return nil, err
		}
	}

	sqsTopic, err := s.config.GenerateSqsQueueName(ctx, snsTopicArn)
	if err != nil {
		return nil, fmt.Errorf("failed to generate SQS queue name: %w", err)
	}

	return s.sqs.Subscribe(ctx, sqsTopic)
}

func (s *Subscriber) SubscribeInitialize(topic string) error {
	return s.SubscribeInitializeWithContext(context.Background(), topic)
}

func (s *Subscriber) SubscribeInitializeWithContext(ctx context.Context, snsTopicArn string) error {
	sqsTopic, err := s.config.GenerateSqsQueueName(ctx, snsTopicArn)
	if err != nil {
		return fmt.Errorf("failed to generate SQS queue name: %w", err)
	}

	if err := s.sqs.SubscribeInitializeWithContext(ctx, sqsTopic); err != nil {
		return fmt.Errorf("cannot initialize SQS subscription for topic %s: %w", snsTopicArn, err)
	}

	sqsURL, err := s.sqs.GetQueueUrl(ctx, sqsTopic)
	if err != nil {
		return fmt.Errorf("cannot get queue url for topic %s: %w", snsTopicArn, err)
	}
	sqsQueueArn, err := s.sqs.GetQueueArn(ctx, sqsURL)
	if err != nil {
		return fmt.Errorf("cannot get queue ARN for topic %s: %w", snsTopicArn, err)
	}

	s.logger.Info("Subscribing to SNS", watermill.LogFields{
		"sns_topic_arn": snsTopicArn,
		"sqs_topic":     sqsTopic,
		"sqs_url":       *sqsURL,
		"sqs_arn":       *sqsQueueArn,
	})

	input, err := s.config.GenerateSubscribeInput(ctx, GenerateSubscribeInputParams{
		SqsTopic:    sqsTopic,
		SnsTopicArn: snsTopicArn,
		SqsQueueArn: *sqsQueueArn,
	})

	subscribeOutput, err := s.sns.Subscribe(ctx, input)
	if err != nil || subscribeOutput == nil {
		return fmt.Errorf("cannot subscribe to SNS[%s] from %s: %w", snsTopicArn, *sqsQueueArn, err)
	}

	return nil
}

func (s *Subscriber) Close() error {
	return s.sqs.Close()
}
