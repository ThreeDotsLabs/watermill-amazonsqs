package main

import (
	"context"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/sns"
)

const SNS_TOPIC = "local-topic1"
const SQS_QUEUE = "local-queue4"

func main() {
	logger := watermill.NewStdLogger(true, true)

	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("eu-north-1"),
		connection.SetEndPoint(os.Getenv("AWS_SNS_ENDPOINT")),
	)
	if err != nil {
		panic(err)
	}

	pub, err := sns.NewPublisher(sns.PublisherConfig{
		AWSConfig:             cfg,
		CreateTopicfNotExists: true,
	}, logger)
	if err != nil {
		panic(err)
	}

	sub, err := sqs.NewSubscriber(sqs.SubscriberConfig{
		AWSConfig: cfg,
	}, logger)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	messages, err := sub.Subscribe(ctx, SQS_QUEUE)
	if err != nil {
		panic(err)
	}

	pubArn, err := pub.GetArnTopic(ctx, SNS_TOPIC)
	if err != nil {
		panic(err)
	}
	sqsUrl, err := sub.GetQueueUrl(ctx, SQS_QUEUE)
	if err != nil {
		panic(err)
	}
	sqsArn, err := sub.GetQueueArn(ctx, sqsUrl)
	if err != nil {
		panic(err)
	}

	err = pub.AddSubscription(ctx, &awssns.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: pubArn,
		Endpoint: sqsArn,
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	})
	if err != nil {
		panic(err)
	}

	// Start consuming messages from SQS
	go func() {
		for m := range messages {
			logger.With(watermill.LogFields{"message": string(m.Payload)}).Info("Received message", nil)
			m.Ack()
		}
	}()
	// Start sending messages to SNS
	for {
		msg := message.NewMessage(watermill.NewULID(), []byte(`{"some_json": "body"}`))
		err := pub.Publish(SNS_TOPIC, msg)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}
