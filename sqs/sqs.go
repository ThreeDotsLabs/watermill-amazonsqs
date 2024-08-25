package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pkg/errors"
)

func getQueueUrl(ctx context.Context, sqsClient *sqs.Client, topic string, input *sqs.GetQueueUrlInput) (*string, error) {
	getQueueOutput, err := sqsClient.GetQueueUrl(ctx, input)

	if err != nil || getQueueOutput.QueueUrl == nil {
		return nil, fmt.Errorf("cannot get queue %s: %w", topic, err)
	}
	return getQueueOutput.QueueUrl, nil
}

// todo: wtf about that?
func createQueue(
	ctx context.Context,
	sqsClient *sqs.Client,
	createQueueParams *sqs.CreateQueueInput,
) (*string, error) {
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
