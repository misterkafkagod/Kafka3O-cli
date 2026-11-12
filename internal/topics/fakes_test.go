package topics

import (
	"context"
	"encoding/json"
	"fmt"

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

var topicReads = map[string]string{
	"/api/clusters": `{"clusters":[{"id":"kafka-b","name":"Kafka B","environment":"PRODUCTION","effectiveRole":"ADMIN"},{"id":"kafka-a","name":"Kafka A","environment":"NON_PRODUCTION","effectiveRole":"ADMIN"}]}`,
	"/api/clusters/kafka-a/gateway/api/v1/topics": `[{"name":"tert","partitions":1,"replicationFactor":1,"internal":false,"approxMessages":2},
		{"name":"kafka3o-m10-verify","partitions":4,"replicationFactor":1,"internal":false,"approxMessages":118},
		{"name":"__consumer_offsets","partitions":50,"replicationFactor":1,"internal":true,"approxMessages":900}]`,
	"/api/clusters/kafka-b/gateway/api/v1/topics": `[{"name":"orders","partitions":3,"replicationFactor":1,"internal":false,"approxMessages":9007199254740993}]`,
}

func across(get *fakeGetJSON) ListTopicsAcrossClusters {
	return NewListTopicsAcrossClusters(clusters.NewListAccessibleClusters(get), NewListClusterTopics(get))
}
