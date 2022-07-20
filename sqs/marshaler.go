package sqs

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Marshaler interface {
	Marshal(msg *message.Message) (types.Message, error)
}

type UnMarshaler interface {
	Unmarshal(msg types.Message) (*message.Message, error)
}

type DefaultMarshalerUnmarshaler struct{}

func (d DefaultMarshalerUnmarshaler) Marshal(msg *message.Message) (types.Message, error) {
	return types.Message{
		MessageAttributes: metadataToAttributes(msg.Metadata),
		Body:              aws.String(string(msg.Payload)),
		MessageId:         aws.String(msg.UUID),
	}, nil
}

func (d DefaultMarshalerUnmarshaler) Unmarshal(msg types.Message) (*message.Message, error) {
	var uuid, payload string

	if msg.MessageId != nil {
		uuid = *msg.MessageId
	}

	if msg.Body != nil {
		payload = *msg.Body
	}

	wmsg := message.NewMessage(uuid, []byte(payload))
	wmsg.Metadata = attributesToMetadata(msg.MessageAttributes)

	return wmsg, nil
}

func metadataToAttributes(meta message.Metadata) map[string]types.MessageAttributeValue {
	attributes := make(map[string]types.MessageAttributeValue)

	for k, v := range meta {
		attributes[k] = types.MessageAttributeValue{
			StringValue: aws.String(v),
			DataType:    aws.String(AWSStringDataType),
		}
	}

	return attributes
}

func attributesToMetadata(attributes map[string]types.MessageAttributeValue) message.Metadata {
	meta := make(message.Metadata)

	for k, v := range attributes {
		if v.DataType == nil {
			continue
		}

		switch *v.DataType {
		case AWSStringDataType, AWSNumberDataType:
			if v.StringValue != nil {
				meta[k] = *v.StringValue
			}
		case AWSBinaryDataType:
			meta[k] = string(v.BinaryValue)
		}
	}

	return meta
}

const (
	AWSStringDataType = "String"
	AWSNumberDataType = "Number"
	AWSBinaryDataType = "Binary"
)
