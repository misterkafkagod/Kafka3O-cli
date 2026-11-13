package messages

import (
	"context"
	"net/url"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ReadMessages reads a topic's latest messages, or the messages from a position (the Gateway returns up to 50).
type ReadMessages interface {
	Execute(ctx context.Context, session console.Session, cluster, topic string, position *Position) ([]Message, error)
}

type readMessages struct {
	get console.GetJSON
}

func NewReadMessages(get console.GetJSON) ReadMessages {
	return readMessages{get: get}
}

func (r readMessages) Execute(ctx context.Context, session console.Session, cluster, topic string, position *Position) ([]Message, error) {
	path := "/api/v1/topics/" + url.PathEscape(topic) + "/messages"
	if position != nil {
		query := url.Values{"partition": {strconv.Itoa(int(position.Partition))}}
		if position.Offset != nil {
			query.Set("offset", strconv.FormatInt(*position.Offset, 10))
		}
		if position.Timestamp != nil {
			query.Set("timestamp", strconv.FormatInt(*position.Timestamp, 10))
		}
		path += "?" + query.Encode()
	}
	var messages []Message
	if err := r.get.Execute(ctx, session, clusters.GatewayPath(cluster, path), &messages); err != nil {
		return nil, err
	}
	if messages == nil {
		messages = []Message{}
	}
	return messages, nil
}
