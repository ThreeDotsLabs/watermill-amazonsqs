package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type QueueConfigAttributes struct {
	DelaySeconds                  string                    `json:"DelaySeconds,omitempty"`
	MaximumMessageSize            string                    `json:"MaximumMessageSize,omitempty"`
	MessageRetentionPeriod        string                    `json:"MessageRetentionPeriod,omitempty"`
	Policy                        string                    `json:"Policy,omitempty"`
	ReceiveMessageWaitTimeSeconds string                    `json:"ReceiveMessageWaitTimeSeconds,omitempty"`
	RedrivePolicy                 string                    `json:"RedrivePolicy,omitempty"`
	DeadLetterTargetArn           string                    `json:"deadLetterTargetArn,omitempty"`
	MaxReceiveCount               string                    `json:"maxReceiveCount,omitempty"`
	VisibilityTimeout             string                    `json:"VisibilityTimeout,omitempty"`
	KmsMasterKeyId                string                    `json:"KmsMasterKeyId,omitempty"`
	KmsDataKeyReusePeriodSeconds  string                    `json:"KmsDataKeyReusePeriodSeconds,omitempty"`
	SqsManagedSseEnabled          string                    `json:"SqsManagedSseEnabled,omitempty"`
	FifoQueue                     QueueConfigAttributesBool `json:"FifoQueue,omitempty"`
	ContentBasedDeduplication     QueueConfigAttributesBool `json:"ContentBasedDeduplication,omitempty"`
	DeduplicationScope            string                    `json:"DeduplicationScope,omitempty"`
	FifoThroughputLimit           string                    `json:"FifoThroughputLimit,omitempty"`
}

// QueueConfigAttributesBool is a custom type for bool values in QueueConfigAttributes
// that supports marshaling to string.
type QueueConfigAttributesBool bool

func (q QueueConfigAttributesBool) MarshalText() ([]byte, error) {
	if q {
		return []byte("true"), nil
	}
	return []byte("false"), nil
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
	if url == nil {
		return nil, fmt.Errorf("queue URL is nil")
	}

	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: url,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get ARN queue %s: %w", *url, err)
	}

	arn := attrResult.Attributes[string(types.QueueAttributeNameQueueArn)]

	return &arn, nil
}
