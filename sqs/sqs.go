package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type QueueConfigAttributes struct {
	DelaySeconds                  string `mapstructure:"DelaySeconds,omitempty"`
	MaximumMessageSize            string `mapstructure:"MaximumMessageSize,omitempty"`
	MessageRetentionPeriod        string `mapstructure:"MessageRetentionPeriod,omitempty"`
	Policy                        string `mapstructure:"Policy,omitempty"`
	ReceiveMessageWaitTimeSeconds string `mapstructure:"ReceiveMessageWaitTimeSeconds,omitempty"`
	RedrivePolicy                 string `mapstructure:"RedrivePolicy,omitempty"`
	DeadLetterTargetArn           string `mapstructure:"deadLetterTargetArn,omitempty"`
	MaxReceiveCount               string `mapstructure:"maxReceiveCount,omitempty"`
	VisibilityTimeout             string `mapstructure:"VisibilityTimeout,omitempty"`
	KmsMasterKeyId                string `mapstructure:"KmsMasterKeyId,omitempty"`
	KmsDataKeyReusePeriodSeconds  string `mapstructure:"KmsDataKeyReusePeriodSeconds,omitempty"`
	SqsManagedSseEnabled          string `mapstructure:"SqsManagedSseEnabled,omitempty"`
	FifoQueue                     bool   `mapstructure:"FifoQueue,omitempty"`
	ContentBasedDeduplication     bool   `mapstructure:"ContentBasedDeduplication,omitempty"`
	DeduplicationScope            string `mapstructure:"DeduplicationScope,omitempty"`
	FifoThroughputLimit           string `mapstructure:"FifoThroughputLimit,omitempty"`
}

func (q QueueConfigAttributes) Attributes() (map[string]string, error) {
	b, err := json.Marshal(q)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal queue attributes (json.Marshal): %w", err)
	}

	var m map[string]string
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal queue attributes (json.Unmarshal): %w", err)
	}

	return m, nil
}

func getQueueUrl(ctx context.Context, sqsClient *sqs.Client, topic string, generateGetQueueUrlInput GenerateGetQueueUrlInputFunc) (*string, error) {
	input, err := generateGetQueueUrlInput(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("cannot generate input for queue %s: %w", topic, err)
	}

	getQueueOutput, err := sqsClient.GetQueueUrl(ctx, input)

	if err != nil || getQueueOutput.QueueUrl == nil {
		return nil, fmt.Errorf("cannot get queue %s: %w", topic, err)
	}
	return getQueueOutput.QueueUrl, nil
}

func greateQueue(ctx context.Context, sqsClient *sqs.Client, createQueueParams *sqs.CreateQueueInput) (*string, error) {
	createQueueOutput, err := sqsClient.CreateQueue(ctx, createQueueParams)
	if err != nil {
		return nil, fmt.Errorf("cannot create queue %w", err)
	}
	if createQueueOutput.QueueUrl == nil {
		return nil, fmt.Errorf("cannot create queue, queueUrl is nil")
	}

	return createQueueOutput.QueueUrl, nil
}

func getARNUrl(ctx context.Context, sqsClient *sqs.Client, url *string) (*string, error) {
	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: url,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get ARN queue %s: %w", url, err)
	}

	arn := attrResult.Attributes[string(types.QueueAttributeNameQueueArn)]

	return &arn, nil
}
