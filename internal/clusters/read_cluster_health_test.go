package clusters

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestReadClusterHealth(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{"/api/clusters/kafka-a/gateway/api/v1/cluster": `{"clusterId":"lhGF","controllerId":1,"brokers":[{"id":1},{"id":2}]}`}}
	health, err := NewReadClusterHealth(get).Execute(context.Background(), console.Session{}, AccessibleCluster{ID: "kafka-a", Name: "Kafka A"})
	if err != nil || health.Status != StatusHealthy || health.ClusterID != "lhGF" || *health.ControllerID != 1 || *health.Brokers != 2 || health.Name != "Kafka A" {
		t.Fatalf("%+v %v", health, err)
	}
}

func TestReadClusterHealthKeepsFailuresAsStatus(t *testing.T) {
	failing := &fakeGetJSON{err: console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"}}
	health, err := NewReadClusterHealth(failing).Execute(context.Background(), console.Session{}, AccessibleCluster{ID: "kafka-b"})
	if err != nil || health.Status != StatusUnavailable || health.Error == "" || health.Brokers != nil || health.ID != "kafka-b" {
		t.Fatalf("%+v %v", health, err)
	}
	if _, err := NewReadClusterHealth(&fakeGetJSON{err: console.ErrSignedOut}).Execute(context.Background(), console.Session{}, AccessibleCluster{ID: "kafka-a"}); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("signed out: %v", err)
	}
}
