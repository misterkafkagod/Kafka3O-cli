package clusters

import (
	"context"
	"sync"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListClusterHealth reads every accessible cluster's health.
type ListClusterHealth interface {
	Execute(ctx context.Context, session console.Session) ([]ClusterHealth, error)
}

type listClusterHealth struct {
	list ListAccessibleClusters
	read ReadClusterHealth
}

func NewListClusterHealth(list ListAccessibleClusters, read ReadClusterHealth) ListClusterHealth {
	return listClusterHealth{list: list, read: read}
}

// Execute reads the clusters concurrently and returns them in the Console's order; only a session that is over
// fails the whole list.
func (l listClusterHealth) Execute(ctx context.Context, session console.Session) ([]ClusterHealth, error) {
	accessible, err := l.list.Execute(ctx, session)
	if err != nil {
		return nil, err
	}
	results := make([]ClusterHealth, len(accessible))
	failures := make([]error, len(accessible))
	var wait sync.WaitGroup
	for i, cluster := range accessible {
		wait.Go(func() {
			results[i], failures[i] = l.read.Execute(ctx, session, cluster)
		})
	}
	wait.Wait()
	for _, failure := range failures {
		if failure != nil {
			return nil, failure
		}
	}
	return results, nil
}
