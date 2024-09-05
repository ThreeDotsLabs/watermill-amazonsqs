package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pkg/errors"
)

type QueueURL string

type QueueName string

type QueueArn string

func getQueueUrl(ctx context.Context, sqsClient *sqs.Client, topic string, input *sqs.GetQueueUrlInput) (*QueueURL, error) {
	getQueueOutput, err := sqsClient.GetQueueUrl(ctx, input)

	if err != nil || getQueueOutput.QueueUrl == nil {
		return nil, fmt.Errorf("cannot get queue %s: %w", topic, err)
	}

	queueURL := QueueURL(*getQueueOutput.QueueUrl)

	return &queueURL, nil
}

func createQueue(
	ctx context.Context,
	sqsClient *sqs.Client,
	createQueueParams *sqs.CreateQueueInput,
) (*QueueURL, error) {
	createQueueOutput, err := sqsClient.CreateQueue(ctx, createQueueParams)
	// possible scenarios:
	// 1. queue already exists, but with different params
	//(for example: "A queue already exists with the same name and a different value for attribute VisibilityTimeout")
	// 2. queue was created in the meantime
	var queueExistsErrr *types.QueueNameExists
	if errors.As(err, &queueExistsErrr) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cannot create queue '%s': %w", *createQueueParams.QueueName, err)
	}
	if createQueueOutput.QueueUrl == nil {
		return nil, fmt.Errorf("cannot create queue, queueUrl is nil")
	}

	queueURL := QueueURL(*createQueueOutput.QueueUrl)

	return &queueURL, nil
}

func getARNUrl(ctx context.Context, sqsClient *sqs.Client, url *QueueURL) (*QueueArn, error) {
	if url == nil {
		return nil, fmt.Errorf("queue URL is nil")
	}

	urlStr := string(*url)

	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &urlStr,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get ARN queue %s: %w", *url, err)
	}

	arn := QueueArn(attrResult.Attributes[string(types.QueueAttributeNameQueueArn)])

	return &arn, nil
}
