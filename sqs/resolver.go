package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go/endpoints"
)

// OverrideEndpointResolver is a custom endpoint resolver that always returns the same endpoint.
// It can be used to use AWS emulators like Localstack.
//
// For example:
// import (
//
//	"github.com/ThreeDotsLabs/watermill-amazonsqs/sqs"
//	amazonsqs "github.com/aws/aws-sdk-go-v2/service/sqs"
//	"github.com/aws/smithy-go/transport"
//
// )
//
//	pub, err := sqs.NewPublisher(sqs.PublisherConfig{
//			AWSConfig: cfg,
//			Marshaler: sqs.DefaultMarshalerUnmarshaler{},
//			OptFns: []func(*amazonsqs.Options){
//				amazonsqs.WithEndpointResolverV2(sqs.OverrideEndpointResolver{
//					Endpoint: transport.Endpoint{
//						URI: url.URL{Scheme: "http", Host: "localstack:4566"},
//					},
//				}),
//			},
//		}, logger)
type OverrideEndpointResolver struct {
	Endpoint transport.Endpoint
}

func (o OverrideEndpointResolver) ResolveEndpoint(ctx context.Context, params sqs.EndpointParameters) (transport.Endpoint, error) {
	return o.Endpoint, nil
}
