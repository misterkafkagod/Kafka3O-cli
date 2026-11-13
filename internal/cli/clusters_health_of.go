package cli

import (
	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
)

// clustersHealthOf turns a cluster's failure into a health row, so it is described like in `kafka3o clusters`.
func clustersHealthOf(failure clusters.ClusterFailure) clusters.ClusterHealth {
	return clusters.ClusterHealth{AccessibleCluster: clusters.AccessibleCluster{ID: failure.Cluster}, Status: failure.Status, Error: failure.Error}
}
