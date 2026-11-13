package topics

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestFixedClusters(t *testing.T) {
	got, err := fixedClusters{{ID: "kafka-a"}}.Execute(context.Background(), console.Session{})
	if err != nil || len(got) != 1 || got[0] != (clusters.AccessibleCluster{ID: "kafka-a"}) {
		t.Fatalf("%v %v", got, err)
	}
}
