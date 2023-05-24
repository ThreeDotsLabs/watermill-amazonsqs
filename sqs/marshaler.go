package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Marshaler interface {
	Marshal(msg *message.Message) (*sqs.Message, error)
}

type UnMarshaler interface {
	Unmarshal(msg *sqs.Message) (*message.Message, error)
}

// UUIDHeaderKey is the key of the Pub/Sub attribute that carries Waterfall UUID.
const UUIDHeaderKey = "_watermill_message_uuid"

type DefaultMarshalerUnmarshaler struct{}

func (d DefaultMarshalerUnmarshaler) Marshal(msg *message.Message) (*sqs.Message, error) {
	attributes := make(map[string]*sqs.MessageAttributeValue, len(msg.Metadata)+1)
	for k, v := range msg.Metadata {
		attributes[k] = &sqs.MessageAttributeValue{
			StringValue: aws.String(v),
			DataType:    aws.String(AWSStringDataType),
		}
	}
	attributes[UUIDHeaderKey] = &sqs.MessageAttributeValue{
		StringValue: aws.String(msg.UUID),
		DataType:    aws.String(AWSStringDataType),
	}

	return &sqs.Message{
		MessageAttributes: attributes,
		Body:              aws.String(string(msg.Payload)),
	}, nil
}

func (d DefaultMarshalerUnmarshaler) Unmarshal(msg *sqs.Message) (*message.Message, error) {
	var payload string
	if msg.Body != nil {
		payload = *msg.Body
	}

	wmsg := message.NewMessage("", []byte(payload))
	wmsg.Metadata = make(message.Metadata, len(msg.MessageAttributes))

	for k, v := range msg.MessageAttributes {
		if v.DataType == nil {
			continue
		}

		if k == UUIDHeaderKey && *v.DataType == AWSStringDataType {
			wmsg.UUID = *v.StringValue
			continue
		}

		switch *v.DataType {
		case AWSStringDataType, AWSNumberDataType:
			if v.StringValue != nil {
				wmsg.Metadata[k] = *v.StringValue
			}
		case AWSBinaryDataType:
			wmsg.Metadata[k] = string(v.BinaryValue)
		}
	}

	return wmsg, nil
}

const (
	AWSStringDataType = "String"
	AWSNumberDataType = "Number"
	AWSBinaryDataType = "Binary"
)
