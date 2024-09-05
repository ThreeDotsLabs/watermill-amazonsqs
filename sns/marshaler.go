package sns

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/ThreeDotsLabs/watermill/message"
)

// todo: check if it can be renamed
const UUIDAttribute = "UUID"

type Marshaler interface {
	Marshal(topicArn TopicArn, msg *message.Message) *sns.PublishInput
}

type DefaultMarshalerUnmarshaler struct{}

func (d DefaultMarshalerUnmarshaler) Marshal(topicArn TopicArn, msg *message.Message) *sns.PublishInput {
	// client side uuid
	// there is a deduplication id that can be use for
	// fifo queues
	// todo: check how it works
	attributes, deduplicationId, groupId := metadataToAttributes(msg.Metadata)
	attributes[UUIDAttribute] = types.MessageAttributeValue{
		StringValue: aws.String(msg.UUID),
		DataType:    aws.String("String"),
	}

	return &sns.PublishInput{
		Message:                aws.String(string(msg.Payload)),
		MessageAttributes:      attributes,
		MessageDeduplicationId: deduplicationId,
		MessageGroupId:         groupId,
		TargetArn:              aws.String(string(topicArn)),
	}
}

func metadataToAttributes(meta message.Metadata) (map[string]types.MessageAttributeValue, *string, *string) {
	attributes := make(map[string]types.MessageAttributeValue)
	var deduplicationId, groupId *string
	for k, v := range meta {
		// SNS has special attributes for deduplication and group id
		if k == "MessageDeduplicationId" {
			deduplicationId = aws.String(v)
			continue
		}
		if k == "MessageGroupId" {
			groupId = aws.String(v)
			continue
		}
		attributes[k] = types.MessageAttributeValue{
			StringValue: aws.String(v),
			DataType:    aws.String("String"),
		}
	}

	return attributes, deduplicationId, groupId
}
