package sqs

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

func TestMetadaConverter(t *testing.T) {
	metadata := message.Metadata{
		"name": "some-event-name",
		"test": "other-value",
	}

	metadataOut := attributesToMetadata(metadataToAttributes(metadata))

	require.Equal(t, metadata, metadataOut, "metadata after transformations should be the same")
}

func TestMarshaler(t *testing.T) {
	msg := message.NewMessage("some-id", []byte("payload"))
	msg.Metadata = message.Metadata{
		"name": "some-event-name",
		"test": "other-value",
	}

	dum := &DefaultMarshalerUnmarshaler{}

	awsMsg, err := dum.Marshal(msg)
	require.NoError(t, err)

	msgOut, err := dum.Unmarshal(awsMsg)
	require.NoError(t, err)

	require.Equal(t, msg.UUID, msgOut.UUID)
	require.Equal(t, msg.Payload, msgOut.Payload)
	require.Equal(t, msg.Metadata, msgOut.Metadata)
}
