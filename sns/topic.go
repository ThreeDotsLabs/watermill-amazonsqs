package sns

import (
	"context"
	"errors"
	"fmt"
)

type TopicResolver interface {
	ResolveTopic(ctx context.Context, topic string) (snsTopic string, err error)
}

type TransparentTopicResolver struct{}

func (a TransparentTopicResolver) ResolveTopic(ctx context.Context, topic string) (snsTopic string, err error) {
	// we are passing topic ARN as topic
	return topic, nil
}

type GenerateArnTopicResolver struct {
	accountID string
	region    string
}

func NewGenerateArnTopicResolver(accountID string, region string) (*GenerateArnTopicResolver, error) {
	var err error

	if accountID == "" {
		err = errors.Join(err, fmt.Errorf("accountID is empty"))
	}
	if region == "" {
		err = errors.Join(err, fmt.Errorf("region is empty"))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create GenerateArnTopicResolver: %w", err)
	}

	return &GenerateArnTopicResolver{accountID: accountID, region: region}, nil
}

func (g GenerateArnTopicResolver) ResolveTopic(ctx context.Context, topic string) (snsTopic string, err error) {
	return GenerateTopicArn(g.region, g.accountID, topic)
}
