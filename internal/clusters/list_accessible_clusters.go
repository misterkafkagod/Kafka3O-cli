package clusters

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListAccessibleClusters returns the clusters the Console lets the signed-in user see, in the Console's order.
type ListAccessibleClusters interface {
	Execute(ctx context.Context, session console.Session) ([]AccessibleCluster, error)
}

type listAccessibleClusters struct {
	get console.GetJSON
}

func NewListAccessibleClusters(get console.GetJSON) ListAccessibleClusters {
	return listAccessibleClusters{get: get}
}

func (l listAccessibleClusters) Execute(ctx context.Context, session console.Session) ([]AccessibleCluster, error) {
	var answer struct {
		Clusters []AccessibleCluster `json:"clusters"`
	}
	if err := l.get.Execute(ctx, session, "/api/clusters", &answer); err != nil {
		return nil, err
	}
	if answer.Clusters == nil {
		return []AccessibleCluster{}, nil
	}
	return answer.Clusters, nil
}
