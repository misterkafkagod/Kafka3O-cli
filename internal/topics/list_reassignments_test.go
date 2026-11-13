package topics

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestListReassignments(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{
		"/api/clusters/kafka-a/gateway/api/v1/topics/t/reassignments": `[{"partition":0,"replicas":[1,2],"addingReplicas":[2],"removingReplicas":[]}]`,
		"/api/clusters/kafka-a/gateway/api/v1/topics/u/reassignments": `[]`,
	}}
	moves, err := NewListReassignments(get).Execute(context.Background(), console.Session{}, "kafka-a", "t")
	if err != nil || len(moves) != 1 || moves[0].AddingReplicas[0] != 2 {
		t.Fatalf("%+v %v", moves, err)
	}
	if none, _ := NewListReassignments(get).Execute(context.Background(), console.Session{}, "kafka-a", "u"); none == nil || len(none) != 0 {
		t.Fatalf("%v", none)
	}
}
