package topics

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestReassignPartition(t *testing.T) {
	send := &fakeSendJSON{answer: `{"topic":"t","status":"accepted"}`}
	if err := NewReassignPartition(send).Execute(context.Background(), console.Session{}, "kafka-a", "t", 0, []int32{2, 1}, "r"); err != nil {
		t.Fatal(err)
	}
	if send.path != "/api/clusters/kafka-a/gateway/api/v1/topics/t/partitions/0/replicas" || send.body != `{"replicas":[2,1]}` || send.reason != "r" {
		t.Fatalf("%+v", send)
	}
}
