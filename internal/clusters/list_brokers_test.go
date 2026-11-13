package clusters

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestListBrokers(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{
		"/api/clusters": `{"clusters":[{"id":"kafka-a"},{"id":"kafka-b"}]}`,
		"/api/clusters/kafka-a/gateway/api/v1/cluster": `{"clusterId":"a","controllerId":2,"brokers":[{"id":1,"host":"a1","port":9092},{"id":2,"host":"a2","port":9093}]}`,
		"/api/clusters/kafka-b/gateway/api/v1/cluster": `{"clusterId":"b","controllerId":1,"brokers":[{"id":1,"host":"b1","port":9092}]}`,
	}}
	brokers, failures, err := NewListBrokers(NewListAccessibleClusters(get), get).Execute(context.Background(), console.Session{}, "")
	if err != nil || len(failures) != 0 || len(brokers) != 3 {
		t.Fatalf("%+v %+v %v", brokers, failures, err)
	}
	if brokers[0] != (Broker{Cluster: "kafka-a", ID: 1, Host: "a1", Port: 9092}) || !brokers[1].Controller || brokers[2] != (Broker{Cluster: "kafka-b", ID: 1, Host: "b1", Port: 9092, Controller: true}) {
		t.Fatalf("%+v", brokers)
	}
}
