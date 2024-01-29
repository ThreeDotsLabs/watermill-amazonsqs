package main

import (
	"context"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/connection"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
)

func main() {
	ctx := context.Background()
	logger := watermill.NewStdLogger(true, true)
	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("eu-north-1"),
		connection.SetEndPoint(os.Getenv("AWS_SNS_ENDPOINT")),
	)
	if err != nil {
		panic(err)
	}
	pub, err := sqs.NewPublisher(sqs.PublisherConfig{
		AWSConfig:              cfg,
		CreateQueueIfNotExists: true,
		Marshaler:              sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	if err != nil {
		panic(err)
	}
	_ = pub

	sub, err := sqs.NewSubscriber(sqs.SubscriberConfig{
		AWSConfig:                    cfg,
		CreateQueueInitializerConfig: sqs.QueueConfigAtrributes{},
		Unmarshaler:                  sqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	if err != nil {
		panic(err)
	}

	err = sub.SubscribeInitialize("any-topic")
	if err != nil {
		panic(err)
	}

	messages, err := sub.Subscribe(ctx, "any-topic")
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
		err := pub.Publish("any-topic", msg)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}
