package sqs

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshaler(t *testing.T) {
	msg := message.NewMessage("some-id", []byte("payload"))
	msg.Metadata = message.Metadata{
		"name": "some-event-name",
		"test": "other-value",
	}

	dum := &DefaultMarshalerUnmarshaler{}

	awsMsg, err := dum.Marshal(msg)
	require.NoError(t, err)
	assert.Equal(t, &sqs.Message{
		Body: aws.String("payload"),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"_watermill_message_uuid": {
				DataType:    aws.String(AWSStringDataType),
				StringValue: aws.String("some-id"),
			},
			"name": {
				DataType:    aws.String(AWSStringDataType),
				StringValue: aws.String("some-event-name"),
			},
			"test": {
				DataType:    aws.String(AWSStringDataType),
				StringValue: aws.String("other-value"),
			},
		},
	}, awsMsg)
}

func TestUnmarshaler(t *testing.T) {
	m := &DefaultMarshalerUnmarshaler{}

	msg, err := m.Unmarshal(&sqs.Message{
		Body: aws.String("payload"),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"_watermill_message_uuid": {
				DataType:    aws.String(AWSStringDataType),
				StringValue: aws.String("some-id"),
			},
			"name": {
				DataType:    aws.String(AWSStringDataType),
				StringValue: aws.String("some-event-name"),
			},
			"test": {
				DataType:    aws.String(AWSStringDataType),
				StringValue: aws.String("other-value"),
			},
			"nil datatype": {
				DataType:    nil,
				StringValue: nil,
			},
			"nil stringvalue": {
				DataType:    aws.String(AWSStringDataType),
				StringValue: nil,
			},
			"numeric": {
				DataType:    aws.String(AWSNumberDataType),
				StringValue: aws.String("1234"),
			},
			"binary": {
				DataType:    aws.String(AWSBinaryDataType),
				BinaryValue: []byte("binary-data"),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "some-id", msg.UUID)
	assert.Equal(t, message.Payload("payload"), msg.Payload)
	assert.Equal(t, message.Metadata{
		"name":    "some-event-name",
		"test":    "other-value",
		"numeric": "1234",
		"binary":  "binary-data",
	}, msg.Metadata)
}
