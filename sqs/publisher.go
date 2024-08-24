package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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

	queuesCache     map[string]*string
	queuesCacheLock sync.RWMutex
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	return &Publisher{
		sqs:         sqs.NewFromConfig(config.AWSConfig),
		config:      config,
		logger:      logger,
		queuesCache: make(map[string]*string),
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	ctx := context.Background()

	queueUrl, err := p.GetOrCreateQueueUrl(ctx, topic)
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

func (p *Publisher) GetOrCreateQueueUrl(ctx context.Context, topic string) (queueUrl *string, err error) {
	getQueueInput, err := p.config.GenerateGetQueueUrlInput(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("cannot generate input for queue %s: %w", topic, err)
	}

	var getQueueInputHash string

	if !p.config.DoNotCacheQueues {
		getQueueInputHash = generateGetQueueUrlInputHash(getQueueInput)

		if getQueueInputHash != "" {
			p.queuesCacheLock.RLock()
			var ok bool
			queueUrl, ok = p.queuesCache[getQueueInputHash]
			p.queuesCacheLock.RUnlock()

			if ok {
				p.logger.Trace("Used cache", watermill.LogFields{
					"topic": topic,
					"queue": *queueUrl,
					"hash":  getQueueInputHash,
				})
				return queueUrl, nil
			}
		}
	}

	defer func() {
		if err == nil && getQueueInputHash != "" {
			p.queuesCacheLock.Lock()
			p.queuesCache[getQueueInputHash] = queueUrl
			p.queuesCacheLock.Unlock()

			p.logger.Trace("Stored cache", watermill.LogFields{
				"topic": topic,
				"queue": *queueUrl,
				"hash":  getQueueInputHash,
			})
		}
	}()

	queueUrl, err = getQueueUrl(ctx, p.sqs, topic, getQueueInput)
	if err == nil {
		return queueUrl, nil
	}

	var queueDoesNotExistsErr *types.QueueDoesNotExist
	if errors.As(err, &queueDoesNotExistsErr) && p.config.CreateQueueIfNotExists {
		input, err := p.config.GenerateCreateQueueInput(ctx, topic, p.config.CreateQueueConfig)
		if err != nil {
			return nil, fmt.Errorf("cannot generate create queue input: %w", err)
		}

		queueUrl, err = createQueue(ctx, p.sqs, input)
		if err != nil {
			return nil, fmt.Errorf("cannot create queue: %w", err)
		}
		// queue was created in the meantime
		if queueUrl == nil {
			return getQueueUrl(ctx, p.sqs, topic, getQueueInput)
		}

		return queueUrl, nil
	} else {
		return nil, fmt.Errorf("cannot get queue url: %w", err)
	}
}

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
