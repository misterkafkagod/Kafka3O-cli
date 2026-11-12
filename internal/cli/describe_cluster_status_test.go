package cli

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
)

func TestDescribeClusterStatus(t *testing.T) {
	for cluster, want := range map[clusters.ClusterHealth]string{
		{Status: clusters.StatusHealthy}:                                               "Healthy",
		{Status: clusters.StatusClusterUnreachable, Error: "kafka: dial tcp: timeout"}: "Cluster unreachable: kafka: dial tcp: timeout",
		{Status: "new_state"}:                                                          "new_state",
	} {
		if got := describeClusterStatus(cluster); got != want {
			t.Errorf("%q, want %q", got, want)
		}
	}
}
