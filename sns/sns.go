package sns

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func createSnsTopic(ctx context.Context, snsClient *sns.Client, createSNSParams sns.CreateTopicInput) (*string, error) {
	createSNSOutput, err := snsClient.CreateTopic(ctx, &createSNSParams)
	if err != nil || createSNSOutput.TopicArn == nil {
		return nil, fmt.Errorf("cannot create SNS topic %s: %w", *createSNSParams.Name, err)
	}
	return createSNSOutput.TopicArn, nil
}

func GenerateTopicArn(region, accountID, topic string) (string, error) {
	var err error
	if region == "" {
		err = errors.Join(err, fmt.Errorf("region is empty"))
	}
	if accountID == "" {
		err = errors.Join(err, fmt.Errorf("accountID is empty"))
	}
	if topic == "" {
		err = errors.Join(err, fmt.Errorf("topic is empty"))
	}
	if err != nil {
		return "", fmt.Errorf("can't generate topic arn: %w", err)
	}

	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, topic), nil
}

func ExtractTopicNameFromTopicArn(topicArn string) (string, error) {
	topicArnParts := strings.Split(topicArn, ":")
	if len(topicArnParts) != 6 {
		return "", fmt.Errorf("topic arn should have 6 segments, has %d (%s)", len(topicArnParts), topicArn)
	}

	topicName := topicArnParts[5]
	return topicName, nil
}
