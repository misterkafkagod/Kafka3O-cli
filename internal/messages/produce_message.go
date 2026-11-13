package messages

import (
	"context"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// NewMessage is what the UI's produce form sends: a text value, an optional text key and text headers in order.
type NewMessage struct {
	Value   string       `json:"value"`
	Key     *string      `json:"key,omitempty"`
	Headers []TextHeader `json:"headers"`
}

type TextHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Written is where the message landed; the offset is the exact decimal offset.
type Written struct {
	Partition int32  `json:"partition"`
	Offset    string `json:"offset"`
}

// ProduceMessage writes one message to a topic through the Console.
type ProduceMessage interface {
	Execute(ctx context.Context, session console.Session, cluster, topic string, message NewMessage, reason string) (Written, error)
}

type produceMessage struct {
	send console.SendJSON
}

func NewProduceMessage(send console.SendJSON) ProduceMessage {
	return produceMessage{send: send}
}

func (p produceMessage) Execute(ctx context.Context, session console.Session, cluster, topic string, message NewMessage, reason string) (Written, error) {
	if message.Headers == nil {
		message.Headers = []TextHeader{}
	}
	var written Written
	path := clusters.GatewayPath(cluster, "/api/v1/topics/"+url.PathEscape(topic)+"/messages")
	if err := p.send.Execute(ctx, session, http.MethodPost, path, message, reason, &written); err != nil {
		return Written{}, err
	}
	return written, nil
}
