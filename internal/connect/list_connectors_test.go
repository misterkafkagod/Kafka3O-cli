package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeGetJSON struct {
	answers map[string]string
	errors  map[string]error
}

func (f *fakeGetJSON) Execute(_ context.Context, _ console.Session, path string, into any) error {
	if err := f.errors[path]; err != nil {
		return err
	}
	answer, ok := f.answers[path]
	if !ok {
		return fmt.Errorf("unexpected path %s", path)
	}
	return json.Unmarshal([]byte(answer), into)
}

const connectorsPath = "/api/clusters/kafka-a/gateway/api/v1/connect/connectors"

func connectorReads() map[string]string {
	return map[string]string{
		"/api/clusters": `{"clusters":[{"id":"kafka-a"},{"id":"kafka-b"}]}`,
		connectorsPath: `[
			{"name":"zeta","type":"sink","state":"RUNNING","workerId":"w:8083","tasks":[{"id":1,"state":"FAILED","workerId":"w:8083","error":"boom"},{"id":0,"state":"RUNNING","workerId":"w:8083"}]},
			{"name":"alpha","type":"source","state":"PAUSED","workerId":"w:8083","tasks":[]}]`,
	}
}

func TestListConnectors(t *testing.T) {
	get := &fakeGetJSON{answers: connectorReads(), errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/connect/connectors": console.Problem{Status: 404, Code: "connect_not_configured", Detail: "Kafka Connect is not configured for this cluster"}}}
	found, failures, err := NewListConnectors(clusters.NewListAccessibleClusters(get), get).Execute(context.Background(), console.Session{}, "")
	if err != nil || len(failures) != 0 || len(found) != 2 {
		t.Fatalf("%+v %+v %v", found, failures, err)
	}
	a, b := found[0], found[1]
	if a.Cluster != "kafka-a" || a.NotConfigured || a.Connectors[0].Name != "alpha" || a.Connectors[1].Tasks[0].ID != 0 || a.Connectors[1].Tasks[1].Error != "boom" {
		t.Fatalf("kafka-a %+v", a)
	}
	if b.Cluster != "kafka-b" || !b.NotConfigured || b.Connectors == nil || len(b.Connectors) != 0 {
		t.Fatalf("kafka-b %+v", b)
	}
}

func TestListConnectorsFailures(t *testing.T) {
	unreachable := &fakeGetJSON{answers: connectorReads(), errors: map[string]error{
		connectorsPath: console.Problem{Status: 503, Code: "connect_unreachable", Title: "Kafka Connect unreachable", Detail: "Kafka Connect is unreachable"}}}
	found, failures, err := NewListConnectors(clusters.NewListAccessibleClusters(unreachable), unreachable).Execute(context.Background(), console.Session{}, "kafka-a")
	if err != nil || len(found) != 0 || len(failures) != 1 || failures[0].Cluster != "kafka-a" || failures[0].Error == "" {
		t.Fatalf("%+v %+v %v", found, failures, err)
	}
	signedOut := &fakeGetJSON{answers: connectorReads(), errors: map[string]error{connectorsPath: console.ErrSignedOut}}
	if _, _, err := NewListConnectors(clusters.NewListAccessibleClusters(signedOut), signedOut).Execute(context.Background(), console.Session{}, "kafka-a"); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("signed out: %v", err)
	}
}
