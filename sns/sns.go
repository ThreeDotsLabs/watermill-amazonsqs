package sns

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type SNSConfigAtrributes struct {
	DeliveryPolicy            string `json:"DeliveryPolicy,omitempty"`
	DisplayName               string `json:"DisplayName,omitempty"`
	Policy                    string `json:"Policy,omitempty"`
	SignatureVersion          string `json:"SignatureVersion,omitempty"`
	TracingConfig             string `json:"TracingConfig,omitempty"`
	KmsMasterKeyId            string `json:"KmsMasterKeyId,omitempty"`
	FifoTopic                 string `json:"FifoTopic,omitempty"`
	ContentBasedDeduplication string `json:"ContentBasedDeduplication,omitempty"`
}

func (s SNSConfigAtrributes) Attributes() map[string]string {
	b, _ := json.Marshal(s)
	var m map[string]string
	_ = json.Unmarshal(b, &m)
	return m
}

func CreateSNS(ctx context.Context, snsClient *sns.Client, topicARN string, createSNSParams sns.CreateTopicInput) (*string, error) {
	createSNSParams.Name = aws.String(topicARN)
	createSNSOutput, err := snsClient.CreateTopic(ctx, &createSNSParams)
	if err != nil || createSNSOutput.TopicArn == nil {
		return nil, fmt.Errorf("cannot create SNS %s: %w", topicARN, err)
	}
	return createSNSOutput.TopicArn, nil
}

func CheckARNTopic(ctx context.Context, snsClient *sns.Client, topicARN string) (*string, error) {
	createSNSOutput, err := snsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: aws.String(topicARN),
	})
	if err != nil || createSNSOutput == nil {
		return nil, fmt.Errorf("cannot create SNS %s: %w", topicARN, err)
	}
	return &topicARN, nil
}
