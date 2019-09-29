package main

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amazonsqs/amazonsqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go/aws"
	"time"
)

func main() {
	logger := watermill.NewStdLogger(true, true)

	cfg := aws.Config{
		Region: aws.String("eu-north-1"),
	}

	pub, err := amazonsqs.NewPublisher(amazonsqs.PublisherConfig{
		AWSConfig: cfg,
		Marshaler: amazonsqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	if err != nil {
		panic(err)
	}

	sub, err := amazonsqs.NewSubsciber(amazonsqs.SubscriberConfig{
		AWSConfig:   cfg,
		Unmarshaler: amazonsqs.DefaultMarshalerUnmarshaler{},
	}, logger)
	if err != nil {
		panic(err)
	}

	_ = pub

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
