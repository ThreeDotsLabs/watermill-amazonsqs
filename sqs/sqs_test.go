package sqs_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
	"github.com/stretchr/testify/require"
)

func TestQueueConfigAttributes_Attributes(t *testing.T) {
	structAttrs := sqs.QueueConfigAttributes{
		DelaySeconds:                  "10",
		MaximumMessageSize:            "20",
		MessageRetentionPeriod:        "20",
		Policy:                        "test",
		ReceiveMessageWaitTimeSeconds: "30",
		RedrivePolicy:                 "test",
		DeadLetterTargetArn:           "test",
		FifoQueue:                     false,
		ContentBasedDeduplication:     true,
	}

	attrs, err := structAttrs.Attributes()
	require.NoError(t, err)

	require.Equal(
		t,
		map[string]string{
			"ContentBasedDeduplication":     "true",
			"DelaySeconds":                  "10",
			"MaximumMessageSize":            "20",
			"MessageRetentionPeriod":        "20",
			"Policy":                        "test",
			"ReceiveMessageWaitTimeSeconds": "30",
			"RedrivePolicy":                 "test",
			"deadLetterTargetArn":           "test",
		},
		attrs,
	)
}
