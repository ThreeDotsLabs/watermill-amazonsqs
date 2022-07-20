package main

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/sns"
)

func main() {
	logger := watermill.NewStdLogger(true, true)

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("eu-north-1"))

	if err != nil {
		panic(err)
	}

	pub, err := sns.NewPublisher(sns.PublisherConfig{
		AWSConfig: cfg,
	}, logger)
	if err != nil {
		panic(err)
	}

	sub, err := sqs.NewSubsciber(sqs.SubscriberConfig{
		AWSConfig:   cfg,
		Unmarshaler: sqs.DefaultMarshalerUnmarshaler{},
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
