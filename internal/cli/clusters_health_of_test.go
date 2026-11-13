package cli

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
)

func TestClustersHealthOf(t *testing.T) {
	health := clustersHealthOf(clusters.ClusterFailure{Cluster: "kafka-b", Status: "gateway_unreachable", Error: "The Gateway could not be reached"})
	if health.ID != "kafka-b" || describeClusterStatus(health) != "Gateway unreachable: The Gateway could not be reached" {
		t.Fatalf("%+v", health)
	}
}
