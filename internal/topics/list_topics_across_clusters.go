package topics

import (
	"context"
	"slices"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListTopicsAcrossClusters reads the topics of one cluster (cluster != "") or of every accessible cluster.
type ListTopicsAcrossClusters interface {
	Execute(ctx context.Context, session console.Session, cluster string) ([]Topic, []clusters.ClusterFailure, error)
}

type listTopicsAcrossClusters struct {
	accessible clusters.ListAccessibleClusters
	list       ListClusterTopics
}

func NewListTopicsAcrossClusters(accessible clusters.ListAccessibleClusters, list ListClusterTopics) ListTopicsAcrossClusters {
	return listTopicsAcrossClusters{accessible: accessible, list: list}
}

// Execute reads the clusters concurrently through clusters.ReadAcrossClusters and orders the topics by the
// Console's cluster order, then by name.
func (l listTopicsAcrossClusters) Execute(ctx context.Context, session console.Session, cluster string) ([]Topic, []clusters.ClusterFailure, error) {
	accessible, err := l.accessible.Execute(ctx, session)
	if err != nil {
		return nil, nil, err
	}
	order := map[string]int{}
	for i, candidate := range accessible {
		order[candidate.ID] = i
	}
	perCluster, failures, err := clusters.ReadAcrossClusters(ctx, session, fixedClusters(accessible), cluster,
		func(ctx context.Context, candidate clusters.AccessibleCluster) ([]Topic, error) {
			return l.list.Execute(ctx, session, candidate.ID)
		})
	if err != nil {
		return nil, nil, err
	}
	topics := []Topic{}
	for _, found := range perCluster {
		topics = append(topics, found...)
	}
	slices.SortFunc(topics, CompareClusterTopics(order))
	return topics, failures, nil
}
