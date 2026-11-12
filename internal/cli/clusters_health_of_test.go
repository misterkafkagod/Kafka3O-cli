package cli

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

func TestClustersHealthOf(t *testing.T) {
	health := clustersHealthOf(topics.ClusterFailure{Cluster: "kafka-b", Status: "gateway_unreachable", Error: "The Gateway could not be reached"})
	if health.ID != "kafka-b" || describeClusterStatus(health) != "Gateway unreachable: The Gateway could not be reached" {
		t.Fatalf("%+v", health)
	}
}
