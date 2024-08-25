package sqs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type QueueUrlResolver interface {
	ResolveQueueUrl(ctx context.Context, params ResolveQueueUrlParams) (queueName string, sqsQueueUrl *string, exists bool, err error)
}

type ResolveQueueUrlParams struct {
	// todo: doc that it's topic in watermill's nomenclature
	Topic     string
	SqsClient *sqs.Client
	Logger    watermill.LoggerAdapter
}

// todo: add alternative one that statically generates queue name

type GenerateQueueUrlResolver struct {
	AwsRegion    string
	AwsAccountID string
}

func (p GenerateQueueUrlResolver) ResolveQueueUrl(ctx context.Context, params ResolveQueueUrlParams) (queueName string, queueUrl *string, exists bool, err error) {
	queueURL := fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s", p.AwsRegion, p.AwsAccountID, params.Topic)

	// todo: how to deal with fact that exists is always true? what if it doesn't exist?
	return params.Topic, &queueURL, true, nil
}

type GetQueueUrlByNameUrlResolverConfig struct {
	DoNotCacheQueues bool

	// GenerateGetQueueUrlInput generates *sqs.GetQueueUrlInput for AWS SDK.
	GenerateGetQueueUrlInput GenerateGetQueueUrlInputFunc
}

// GetQueueUrlByNameUrlResolver resolves queue url by calling AWS API.
// todo: more docs
type GetQueueUrlByNameUrlResolver struct {
	config GetQueueUrlByNameUrlResolverConfig

	queuesCache     map[string]*string
	queuesCacheLock sync.RWMutex
}

func NewGetQueueUrlByNameUrlResolver(
	config GetQueueUrlByNameUrlResolverConfig,
) *GetQueueUrlByNameUrlResolver {
	if config.GenerateGetQueueUrlInput == nil {
		config.GenerateGetQueueUrlInput = GenerateGetQueueUrlInputDefault
	}

	return &GetQueueUrlByNameUrlResolver{
		config:      config,
		queuesCache: map[string]*string{},
	}
}

func (p *GetQueueUrlByNameUrlResolver) ResolveQueueUrl(ctx context.Context, params ResolveQueueUrlParams) (queueName string, queueUrl *string, exists bool, err error) {
	getQueueInput, err := p.config.GenerateGetQueueUrlInput(ctx, params.Topic)
	if err != nil {
		return params.Topic, nil, false, fmt.Errorf("cannot generate input for queue %s: %w", params.Topic, err)
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
				params.Logger.Trace("Used cache", watermill.LogFields{
					"topic": params.Topic,
					"queue": *queueUrl,
					"hash":  getQueueInputHash,
				})
				return params.Topic, queueUrl, true, nil
			}
		}
	}

	defer func() {
		if err == nil && exists && getQueueInputHash != "" {
			p.queuesCacheLock.Lock()
			p.queuesCache[getQueueInputHash] = queueUrl
			p.queuesCacheLock.Unlock()

			params.Logger.Trace("Stored cache", watermill.LogFields{
				"topic": params.Topic,
				"queue": *queueUrl,
				"hash":  getQueueInputHash,
			})
		}
	}()

	queueUrl, err = getQueueUrl(ctx, params.SqsClient, params.Topic, getQueueInput)
	if err == nil {
		return params.Topic, queueUrl, true, nil
	}
	var queueDoesNotExistsErr *types.QueueDoesNotExist
	if errors.As(err, &queueDoesNotExistsErr) {
		// todo: this is not a good idea?
		return params.Topic, queueUrl, false, nil
	}

	return params.Topic, nil, false, err
}

type GenerateGetQueueUrlInputFunc func(ctx context.Context, topic string) (*sqs.GetQueueUrlInput, error)

func GenerateGetQueueUrlInputDefault(ctx context.Context, topic string) (*sqs.GetQueueUrlInput, error) {
	return &sqs.GetQueueUrlInput{
		QueueName: aws.String(topic),
	}, nil
}

// todo: test
type TransparentUrlResolver struct{}

func (p TransparentUrlResolver) ResolveQueueUrl(ctx context.Context, params ResolveQueueUrlParams) (queueName string, queueUrl *string, exists bool, err error) {
	topicParts := strings.Split(params.Topic, "/")
	queueName = topicParts[len(topicParts)-1]

	return queueName, &params.Topic, true, nil
}
