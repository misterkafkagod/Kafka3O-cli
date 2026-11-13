package connect

import (
	"context"
	"errors"
	"slices"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListConnectors reads the Kafka Connect connectors of one cluster or of every accessible cluster.
type ListConnectors interface {
	Execute(ctx context.Context, session console.Session, cluster string) ([]ClusterConnectors, []clusters.ClusterFailure, error)
}

type listConnectors struct {
	accessible clusters.ListAccessibleClusters
	get        console.GetJSON
}

func NewListConnectors(accessible clusters.ListAccessibleClusters, get console.GetJSON) ListConnectors {
	return listConnectors{accessible: accessible, get: get}
}

// Execute keeps the Console's cluster order; a cluster without Connect is NotConfigured, not a failure.
func (l listConnectors) Execute(ctx context.Context, session console.Session, cluster string) ([]ClusterConnectors, []clusters.ClusterFailure, error) {
	return clusters.ReadAcrossClusters(ctx, session, l.accessible, cluster,
		func(ctx context.Context, candidate clusters.AccessibleCluster) (ClusterConnectors, error) {
			result := ClusterConnectors{Cluster: candidate.ID, Connectors: []Connector{}}
			err := l.get.Execute(ctx, session, clusters.GatewayPath(candidate.ID, "/api/v1/connect/connectors"), &result.Connectors)
			var problem console.Problem
			if errors.As(err, &problem) && problem.Code == "connect_not_configured" {
				return ClusterConnectors{Cluster: candidate.ID, NotConfigured: true, Connectors: []Connector{}}, nil
			}
			if err != nil {
				return ClusterConnectors{}, err
			}
			slices.SortFunc(result.Connectors, CompareConnectors)
			for _, connector := range result.Connectors {
				slices.SortFunc(connector.Tasks, CompareTasks)
			}
			return result, nil
		})
}
