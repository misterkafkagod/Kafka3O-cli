package clusters

import (
	"context"
	"errors"
	"sync"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ReadAcrossClusters runs read for the chosen cluster (cluster != "") or for every accessible cluster, concurrently.
// Results come back in the Console's order; a cluster that cannot answer becomes a ClusterFailure next to the
// others. Only a signed-out session or an unknown cluster fails the whole read.
func ReadAcrossClusters[T any](ctx context.Context, session console.Session, accessible ListAccessibleClusters, cluster string,
	read func(ctx context.Context, cluster AccessibleCluster) (T, error)) ([]T, []ClusterFailure, error) {
	all, err := accessible.Execute(ctx, session)
	if err != nil {
		return nil, nil, err
	}
	chosen := []AccessibleCluster{}
	for _, candidate := range all {
		if cluster == "" || candidate.ID == cluster {
			chosen = append(chosen, candidate)
		}
	}
	if cluster != "" && len(chosen) == 0 {
		return nil, nil, ErrUnknownCluster{Cluster: cluster}
	}
	results := make([]T, len(chosen))
	failures := make([]error, len(chosen))
	var wait sync.WaitGroup
	for i, candidate := range chosen {
		wait.Go(func() {
			results[i], failures[i] = read(ctx, candidate)
		})
	}
	wait.Wait()
	answered, failed := []T{}, []ClusterFailure{}
	for i, candidate := range chosen {
		switch {
		case errors.Is(failures[i], console.ErrSignedOut):
			return nil, nil, failures[i]
		case failures[i] != nil:
			status, reason := ClassifyClusterError(failures[i])
			failed = append(failed, ClusterFailure{Cluster: candidate.ID, Status: status, Error: reason})
		default:
			answered = append(answered, results[i])
		}
	}
	return answered, failed, nil
}
