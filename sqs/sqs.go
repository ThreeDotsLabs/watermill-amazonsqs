package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type QueueConfigAtrributes struct {
	DelaySeconds                  string `json:"DelaySeconds,omitempty"`
	MaximumMessageSize            string `json:"MaximumMessageSize,omitempty"`
	MessageRetentionPeriod        string `json:"MessageRetentionPeriod,omitempty"`
	Policy                        string `json:"Policy,omitempty"`
	ReceiveMessageWaitTimeSeconds string `json:"ReceiveMessageWaitTimeSeconds,omitempty"`
	RedrivePolicy                 string `json:"RedrivePolicy,omitempty"`
	DeadLetterTargetArn           string `json:"deadLetterTargetArn,omitempty"`
	MaxReceiveCount               string `json:"maxReceiveCount,omitempty"`
	VisibilityTimeout             string `json:"VisibilityTimeout,omitempty"`
	KmsMasterKeyId                string `json:"KmsMasterKeyId,omitempty"`
	KmsDataKeyReusePeriodSeconds  string `json:"KmsDataKeyReusePeriodSeconds,omitempty"`
	SqsManagedSseEnabled          string `json:"SqsManagedSseEnabled,omitempty"`
	FifoQueue                     bool   `json:"FifoQueue,omitempty"`
	ContentBasedDeduplication     bool   `json:"ContentBasedDeduplication,omitempty"`
	DeduplicationScope            string `json:"DeduplicationScope,omitempty"`
	FifoThroughputLimit           string `json:"FifoThroughputLimit,omitempty"`
}

func (q QueueConfigAtrributes) Attributes() map[string]string {
	b, _ := json.Marshal(q)
	var m map[string]string
	_ = json.Unmarshal(b, &m)
	return m
}

func GetQueueUrl(ctx context.Context, sqsClient *sqs.Client, topic string) (*string, error) {
	getQueueOutput, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(topic),
	})

	if err != nil || getQueueOutput.QueueUrl == nil {
		return nil, fmt.Errorf("cannot get queue %s: %w", topic, err)
	}
	return getQueueOutput.QueueUrl, nil
}

func CreateQueue(ctx context.Context, sqsClient *sqs.Client, topic string, createQueueParams sqs.CreateQueueInput) (*string, error) {
	createQueueParams.QueueName = aws.String(topic)
	createQueueOutput, err := sqsClient.CreateQueue(ctx, &createQueueParams)
	if err != nil || createQueueOutput.QueueUrl == nil {
		return nil, fmt.Errorf("cannot create queue %s: %w", topic, err)
	}
	return createQueueOutput.QueueUrl, nil
}

func GetOrCreateQueue(ctx context.Context, sqsClient *sqs.Client, topic string, createQueueParams sqs.CreateQueueInput) (*string, error) {
	queueUrl, err := GetQueueUrl(ctx, sqsClient, topic)
	if err != nil || queueUrl == nil {
		var qne *types.QueueDoesNotExist
		if errors.As(err, &qne) {
			return CreateQueue(ctx, sqsClient, topic, createQueueParams)
		}
	}
	return queueUrl, nil
}

func GetARNUrl(ctx context.Context, sqsClient *sqs.Client, url *string) (*string, error) {
	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: url,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get ARN queue %v: %w", url, err)
	}

	arn := attrResult.Attributes[string(types.QueueAttributeNameQueueArn)]

	return &arn, nil
}
