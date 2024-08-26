package sns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type PublisherConfig struct {
	AWSConfig aws.Config

	CreateTopicConfig           ConfigAttributes
	DoNotCreateTopicIfNotExists bool

	TopicResolver TopicResolver

	GenerateCreateTopicInput GenerateCreateTopicInputFunc

	Marshaler Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshalerUnmarshaler{}
	}

	if c.GenerateCreateTopicInput == nil {
		c.GenerateCreateTopicInput = GenerateCreateTopicInputDefault
	}
}

func (c *PublisherConfig) Validate() error {
	var err error

	if c.AWSConfig.Credentials == nil {
		err = errors.Join(err, fmt.Errorf("sns.PublisherConfig.AWSConfig.Credentials is nil"))
	}
	if c.TopicResolver == nil {
		err = errors.Join(err, fmt.Errorf("sns.PublisherConfig.TopicResolver is nil"))
	}

	return err
}

type GenerateCreateTopicInputFunc func(ctx context.Context, topic string, attrs ConfigAttributes) (sns.CreateTopicInput, error)

func GenerateCreateTopicInputDefault(ctx context.Context, topic string, attrs ConfigAttributes) (sns.CreateTopicInput, error) {
	attrsMap, err := attrs.Attributes()
	if err != nil {
		return sns.CreateTopicInput{}, fmt.Errorf("cannot generate attributes for topic %s: %w", topic, err)
	}

	return sns.CreateTopicInput{
		Name:       aws.String(topic),
		Attributes: attrsMap,
	}, nil
}

type SubscriberConfig struct {
	AWSConfig aws.Config

	// todo: better name?
	GenerateSqsQueueName func(ctx context.Context, snsTopic string) (string, error)

	TopicResolver TopicResolver

	GenerateSubscribeInput GenerateSubscribeInputFn

	DoNotSubscribeToSns bool
}

func (c *SubscriberConfig) SetDefaults() {
	if c.GenerateSubscribeInput == nil {
		c.GenerateSubscribeInput = GenerateSubscribeInputDefault
	}
}

func (c *SubscriberConfig) Validate() error {
	var err error

	if c.AWSConfig.Credentials == nil {
		err = errors.Join(err, fmt.Errorf("sns.SubscriberConfig.AWSConfig.Credentials is nil"))
	}
	if c.GenerateSqsQueueName == nil {
		err = errors.Join(err, fmt.Errorf("sns.SubscriberConfig.GenerateSqsQueueName is nil"))
	}
	if c.TopicResolver == nil {
		err = errors.Join(err, fmt.Errorf("sns.SubscriberConfig.TopicResolver is nil"))
	}

	return err
}

func GenerateSqsQueueNameEqualToTopicName(ctx context.Context, snsTopic string) (string, error) {
	topicName, err := ExtractTopicNameFromTopicArn(snsTopic)
	if err != nil {
		return "", err
	}

	return topicName, nil
}

type GenerateSubscribeInputFn func(ctx context.Context, params GenerateSubscribeInputParams) (*sns.SubscribeInput, error)

type GenerateSubscribeInputParams struct {
	SqsTopic string

	SnsTopicArn string
	SqsQueueArn string
}

func GenerateSubscribeInputDefault(ctx context.Context, params GenerateSubscribeInputParams) (*sns.SubscribeInput, error) {
	return &sns.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: &params.SnsTopicArn,
		Endpoint: &params.SqsQueueArn,
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	}, nil
}

// ConfigAttributes is a struct that holds the attributes of an SNS topic
type ConfigAttributes struct {
	// DeliveryPolicy – The policy that defines how Amazon SNS retries failed
	// deliveries to HTTP/S endpoints.
	DeliveryPolicy string `json:"DeliveryPolicy,omitempty"`

	// DisplayName – The display name to use for a topic with SMS subscriptions.
	DisplayName string `json:"DisplayName,omitempty"`

	// Policy – The policy that defines who can access your topic. By default, only
	// the topic owner can publish or subscribe to the topic.
	Policy string `json:"Policy,omitempty"`

	// SignatureVersion – The signature version corresponds to the hashing
	// algorithm used while creating the signature of the notifications, subscription
	// confirmations, or unsubscribe confirmation messages sent by Amazon SNS. By
	// default, SignatureVersion is set to 1 .
	SignatureVersion string `json:"SignatureVersion,omitempty"`

	// TracingConfig – Tracing mode of an Amazon SNS topic.
	// By default TracingConfig is set to PassThrough , and the topic passes through the tracing
	// header it receives from an Amazon SNS publisher to its subscriptions. If set to
	// Active , Amazon SNS will vend X-Ray segment data to topic owner account if the
	// sampled flag in the tracing header is true. This is only supported on standard
	// topics.
	TracingConfig string `json:"TracingConfig,omitempty"`

	// KmsMasterKeyId – The ID of an Amazon Web Services managed customer master
	// key (CMK) for Amazon SNS or a custom CMK. For more information, see [Key Terms]. For
	// more examples, see [KeyId]in the Key Management Service API Reference.
	//
	// Applies only to server-side encryption.
	KmsMasterKeyId string `json:"KmsMasterKeyId,omitempty"`

	// FifoTopic – Set to true to create a FIFO topic.
	FifoTopic string `json:"FifoTopic,omitempty"`

	// ArchivePolicy – Adds or updates an inline policy document to archive  messages stored in the specified
	// Amazon SNS topic.
	ArchivePolicy string `json:"ArchivePolicy,omitempty"`

	// BeginningArchiveTime – The earliest starting point at which a message in the
	// topic’s archive can be replayed from. This point in time is based on the
	// configured message retention period set by the topic’s message archiving policy.
	BeginningArchiveTime string `json:"BeginningArchiveTime,omitempty"`

	// ContentBasedDeduplication – Enables content-based deduplication for FIFO topics.
	//
	// By default, ContentBasedDeduplication is set to false . If you create a FIFO
	// topic and this attribute is false , you must specify a value for the
	// MessageDeduplicationId parameter for the [Publish]action.
	//
	// When you set ContentBasedDeduplication to true , Amazon SNS uses a SHA-256
	// hash to generate the MessageDeduplicationId using the body of the message (but
	// not the attributes of the message).
	ContentBasedDeduplication string `json:"ContentBasedDeduplication,omitempty"`

	// CustomAttributes is a map of custom attributes that are not mapped to the struct fields.
	CustomAttributes map[string]string `json:"-"`
}

func (s ConfigAttributes) Attributes() (map[string]string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal attributes (json.Marshal): %w", err)
	}

	var m map[string]string
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal attributes (json.Unmarshal): %w", err)
	}

	if s.CustomAttributes != nil {
		for k, v := range s.CustomAttributes {
			m[k] = v
		}
	}

	return m, nil
}