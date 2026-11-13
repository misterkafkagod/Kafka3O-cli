package cli

import (
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// describeError turns DescribeTopic's cluster failure into the command's exit-2 error; other errors stay.
func describeError(err error) error {
	var clusterRead topics.ErrClusterRead
	if errors.As(err, &clusterRead) {
		failure := clusters.ClusterFailure{Cluster: clusterRead.Cluster, Status: clusterRead.Status, Error: clusterRead.Reason}
		return fmt.Errorf("%s: %s: %w", clusterRead.Cluster, describeClusterStatus(clustersHealthOf(failure)), ErrClusterFailed)
	}
	return err
}
