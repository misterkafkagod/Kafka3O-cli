package topics

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListClusterTopics reads one cluster's topics from its Gateway.
type ListClusterTopics interface {
	Execute(ctx context.Context, session console.Session, cluster string) ([]Topic, error)
}

type listClusterTopics struct {
	get console.GetJSON
}

func NewListClusterTopics(get console.GetJSON) ListClusterTopics {
	return listClusterTopics{get: get}
}

func (l listClusterTopics) Execute(ctx context.Context, session console.Session, cluster string) ([]Topic, error) {
	var topics []Topic
	if err := l.get.Execute(ctx, session, clusters.GatewayPath(cluster, "/api/v1/topics"), &topics); err != nil {
		return nil, err
	}
	for i := range topics {
		topics[i].Cluster = cluster
	}
	return topics, nil
}
