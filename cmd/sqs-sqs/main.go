package main

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go/aws"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
)

func main() {
	logger := watermill.NewStdLogger(true, true)

	cfg := aws.Config{
		Region: aws.String("eu-north-1"),
	}

	pub, err := sqs.NewPublisher(sqs.PublisherConfig{
		AWSConfig: cfg,
		Marshaler: sqs.DefaultMarshalerUnmarshaler{},
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
