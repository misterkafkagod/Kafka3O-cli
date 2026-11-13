package environments

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListRegisteredClusters reads every registered cluster with its environment (local global ADMIN only).
type ListRegisteredClusters interface {
	Execute(ctx context.Context, session console.Session) (Listing, error)
}

type listRegisteredClusters struct {
	get console.GetJSON
}

func NewListRegisteredClusters(get console.GetJSON) ListRegisteredClusters {
	return listRegisteredClusters{get: get}
}

func (l listRegisteredClusters) Execute(ctx context.Context, session console.Session) (Listing, error) {
	listing := Listing{Clusters: []RegisteredCluster{}}
	if err := l.get.Execute(ctx, session, "/api/admin/clusters", &listing); err != nil {
		return Listing{}, err
	}
	return listing, nil
}
