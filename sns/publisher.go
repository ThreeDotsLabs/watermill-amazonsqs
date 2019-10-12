package sns

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
)

type PublisherConfig struct {
	AWSConfig aws.Config
}

type Publisher struct {
	config PublisherConfig
	logger watermill.LoggerAdapter
	sns    *sns.SNS
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.AWSConfig = connection.SetEndPoint(config.AWSConfig)
	sess, err := session.NewSession(&config.AWSConfig)
	if err != nil {
		// TODO wrap
		return nil, err
	}

	return &Publisher{
		sns:    sns.New(sess),
		config: config,
		logger: logger,
	}, nil
}

func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	// TODO method for generating
	topicInfo, err := p.sns.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(topic),
	})
	if err != nil {
		return err
	}

	for _, msg := range messages {
		p.logger.Info("Sending message", watermill.LogFields{"msg": msg})
		_, err = p.sns.Publish(&sns.PublishInput{
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
