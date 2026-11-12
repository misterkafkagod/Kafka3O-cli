package topics

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestListClusterTopicsMarksTheCluster(t *testing.T) {
	topics, err := NewListClusterTopics(&fakeGetJSON{answers: topicReads}).Execute(context.Background(), console.Session{}, "kafka-b")
	if err != nil || len(topics) != 1 || topics[0] != (Topic{Cluster: "kafka-b", Name: "orders", Partitions: 3, ReplicationFactor: 1, ApproxMessages: 9007199254740993}) {
		t.Fatalf("%+v %v", topics, err)
	}
}
