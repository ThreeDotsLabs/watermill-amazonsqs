package main

import (
	"context"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/sns"
)

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

	messages, err := sub.Subscribe(ctx, "local-queue4")
	if err != nil {
		panic(err)
	}

	go func() {
		for m := range messages {
			logger.With(watermill.LogFields{"message": m}).Info("Received message", nil)
			m.Ack()
		}
	}()

	for {
		msg := message.NewMessage(watermill.NewULID(), []byte(`{"some_json": "body"}`))
		err := pub.Publish("local-topic1", msg)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}
