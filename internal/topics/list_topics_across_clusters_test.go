package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func names(topics []Topic) []string {
	out := []string{}
	for _, topic := range topics {
		out = append(out, topic.Cluster+"/"+topic.Name)
	}
	return out
}

func TestListTopicsAcrossClustersOrdersByClusterThenName(t *testing.T) {
	topics, failures, err := across(&fakeGetJSON{answers: topicReads}).Execute(context.Background(), console.Session{}, "")
	got := names(topics)
	want := []string{"kafka-b/orders", "kafka-a/__consumer_offsets", "kafka-a/kafka3o-m10-verify", "kafka-a/tert"}
	if err != nil || len(failures) != 0 || len(got) != len(want) {
		t.Fatalf("%v %v %v", got, failures, err)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order %v, want %v", got, want)
		}
	}
}

func TestListTopicsAcrossClustersKeepsAFailingCluster(t *testing.T) {
	get := &fakeGetJSON{answers: topicReads, errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/topics": console.Problem{Status: 503, Code: "cluster_unreachable", Title: "Cluster unreachable", Detail: "kafka: timeout"},
	}}
	topics, failures, err := across(get).Execute(context.Background(), console.Session{}, "")
	if err != nil || len(topics) != 3 || len(failures) != 1 || failures[0] != (ClusterFailure{Cluster: "kafka-b", Status: "cluster_unreachable", Error: "kafka: timeout"}) {
		t.Fatalf("%v %+v %v", names(topics), failures, err)
	}
}

func TestListTopicsAcrossClustersForOneCluster(t *testing.T) {
	get := &fakeGetJSON{answers: topicReads}
	topics, _, err := across(get).Execute(context.Background(), console.Session{}, "kafka-b")
	if err != nil || len(topics) != 1 || topics[0].Name != "orders" {
		t.Fatalf("%v %v", names(topics), err)
	}
	var unknown ErrUnknownCluster
	if _, _, err := across(get).Execute(context.Background(), console.Session{}, "nope"); !errors.As(err, &unknown) || err.Error() != "cluster nope not found or not accessible" {
		t.Fatalf("unknown: %v", err)
	}
}

func TestListTopicsAcrossClustersFailsForASignedOutSession(t *testing.T) {
	get := &fakeGetJSON{answers: topicReads, errors: map[string]error{"/api/clusters/kafka-a/gateway/api/v1/topics": console.ErrSignedOut}}
	if _, _, err := across(get).Execute(context.Background(), console.Session{}, ""); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("%v", err)
	}
}
