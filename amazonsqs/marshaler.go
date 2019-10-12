package amazonsqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Marshaler interface {
	Marshal(msg *message.Message) (*sqs.Message, error)
}

type Unmarshaler interface {
	Unmarshal(msg *sqs.Message) (*message.Message, error)
}

type DefaultMarshalerUnmarshaler struct{}

func (d DefaultMarshalerUnmarshaler) Marshal(msg *message.Message) (*sqs.Message, error) {
	return &sqs.Message{
		Body:      aws.String(string(msg.Payload)),
		MessageId: aws.String(msg.UUID),
	}, nil
}

func (d DefaultMarshalerUnmarshaler) Unmarshal(msg *sqs.Message) (*message.Message, error) {
	// TODO nil check
	uuid := *msg.MessageId
	payload := *msg.Body

	return message.NewMessage(uuid, []byte(payload)), nil
}
