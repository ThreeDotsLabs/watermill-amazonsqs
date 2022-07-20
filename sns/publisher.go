package sns

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
)

type PublisherConfig struct {
	AWSConfig aws.Config
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter
	sns    *sns.Client
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.AWSConfig = connection.SetEndPoint(config.AWSConfig)
	return &Publisher{
		sns:    sns.NewFromConfig(config.AWSConfig),
		config: config,
		logger: logger,
	}, nil
}

func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	// TODO method for generating
	topicInfo, err := p.sns.CreateTopic(context.Background(), &sns.CreateTopicInput{
		Name: aws.String(topic),
	})
	if err != nil {
		return err
	}

	for _, msg := range messages {
		p.logger.Debug("Sending message", watermill.LogFields{"msg": msg})
		_, err = p.sns.Publish(context.Background(), &sns.PublishInput{
			TopicArn: topicInfo.TopicArn,
			Message:  aws.String(string(msg.Payload)),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (p Publisher) Close() error {
	return nil
}
