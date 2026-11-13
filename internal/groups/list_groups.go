package groups

import (
	"context"
	"slices"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListGroups reads the consumer groups of one cluster or of every accessible cluster.
type ListGroups interface {
	Execute(ctx context.Context, session console.Session, cluster string) ([]ConsumerGroup, []clusters.ClusterFailure, error)
}

type listGroups struct {
	accessible clusters.ListAccessibleClusters
	get        console.GetJSON
}

func NewListGroups(accessible clusters.ListAccessibleClusters, get console.GetJSON) ListGroups {
	return listGroups{accessible: accessible, get: get}
}

// Execute keeps the Console's cluster order and sorts each cluster's groups by id.
func (l listGroups) Execute(ctx context.Context, session console.Session, cluster string) ([]ConsumerGroup, []clusters.ClusterFailure, error) {
	perCluster, failures, err := clusters.ReadAcrossClusters(ctx, session, l.accessible, cluster,
		func(ctx context.Context, candidate clusters.AccessibleCluster) ([]ConsumerGroup, error) {
			var groups []ConsumerGroup
			if err := l.get.Execute(ctx, session, clusters.GatewayPath(candidate.ID, "/api/v1/consumer-groups"), &groups); err != nil {
				return nil, err
			}
			for i := range groups {
				groups[i].Cluster = candidate.ID
			}
			slices.SortFunc(groups, CompareGroups)
			return groups, nil
		})
	if err != nil {
		return nil, nil, err
	}
	all := []ConsumerGroup{}
	for _, groups := range perCluster {
		all = append(all, groups...)
	}
	return all, failures, nil
}
