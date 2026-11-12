package topics

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListTopicsAcrossClusters reads the topics of one cluster (cluster != "") or of every accessible cluster.
type ListTopicsAcrossClusters interface {
	Execute(ctx context.Context, session console.Session, cluster string) ([]Topic, []ClusterFailure, error)
}

type listTopicsAcrossClusters struct {
	accessible clusters.ListAccessibleClusters
	list       ListClusterTopics
}

func NewListTopicsAcrossClusters(accessible clusters.ListAccessibleClusters, list ListClusterTopics) ListTopicsAcrossClusters {
	return listTopicsAcrossClusters{accessible: accessible, list: list}
}

// Execute reads the clusters concurrently; a cluster that cannot answer becomes a failure next to the others'
// topics. Only a signed-out session or an unknown cluster fails the whole read.
func (l listTopicsAcrossClusters) Execute(ctx context.Context, session console.Session, cluster string) ([]Topic, []ClusterFailure, error) {
	accessible, err := l.accessible.Execute(ctx, session)
	if err != nil {
		return nil, nil, err
	}
	order := map[string]int{}
	chosen := []string{}
	for i, candidate := range accessible {
		order[candidate.ID] = i
		if cluster == "" || candidate.ID == cluster {
			chosen = append(chosen, candidate.ID)
		}
	}
	if cluster != "" && len(chosen) == 0 {
		return nil, nil, ErrUnknownCluster{Cluster: cluster}
	}
	results := make([][]Topic, len(chosen))
	errs := make([]error, len(chosen))
	var wait sync.WaitGroup
	for i, id := range chosen {
		wait.Go(func() {
			results[i], errs[i] = l.list.Execute(ctx, session, id)
		})
	}
	wait.Wait()
	topics, failures := []Topic{}, []ClusterFailure{}
	for i, id := range chosen {
		switch {
		case errors.Is(errs[i], console.ErrSignedOut):
			return nil, nil, errs[i]
		case errs[i] != nil:
			status, reason := clusters.ClassifyClusterError(errs[i])
			failures = append(failures, ClusterFailure{Cluster: id, Status: status, Error: reason})
		default:
			topics = append(topics, results[i]...)
		}
	}
	slices.SortFunc(topics, CompareClusterTopics(order))
	return topics, failures, nil
}
