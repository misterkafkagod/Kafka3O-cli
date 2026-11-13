package clusters

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// RequireAccessibleCluster fails with ErrUnknownCluster unless the signed-in user may access the cluster.
func RequireAccessibleCluster(ctx context.Context, session console.Session, accessible ListAccessibleClusters, cluster string) error {
	all, err := accessible.Execute(ctx, session)
	if err != nil {
		return err
	}
	for _, candidate := range all {
		if candidate.ID == cluster {
			return nil
		}
	}
	return ErrUnknownCluster{Cluster: cluster}
}
