package messages

import (
	"context"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// SearchResult is the Gateway's bounded search: the matching messages among the most recent ones it scanned.
type SearchResult struct {
	Messages  []Message `json:"messages"`
	Scanned   int       `json:"scanned"`
	Truncated bool      `json:"truncated"`
}

// SearchMessages asks the Gateway for a topic's recent messages whose key or value contains the text.
type SearchMessages interface {
	Execute(ctx context.Context, session console.Session, cluster, topic, text string) (SearchResult, error)
}

type searchMessages struct {
	get console.GetJSON
}

func NewSearchMessages(get console.GetJSON) SearchMessages {
	return searchMessages{get: get}
}

func (s searchMessages) Execute(ctx context.Context, session console.Session, cluster, topic, text string) (SearchResult, error) {
	path := "/api/v1/topics/" + url.PathEscape(topic) + "/messages/search?" + url.Values{"query": {text}}.Encode()
	var result SearchResult
	if err := s.get.Execute(ctx, session, clusters.GatewayPath(cluster, path), &result); err != nil {
		return SearchResult{}, err
	}
	if result.Messages == nil {
		result.Messages = []Message{}
	}
	return result, nil
}
