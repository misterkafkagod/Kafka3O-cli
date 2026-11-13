package groups

import (
	"context"
	"encoding/json"
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

var groupReads = map[string]string{
	"/api/clusters": `{"clusters":[{"id":"kafka-b"},{"id":"kafka-a"}]}`,
	"/api/clusters/kafka-a/gateway/api/v1/consumer-groups": `[
		{"id":"zeta","protocol":"classic","state":"Empty","members":[],"topics":["t"],"partitions":[{"topic":"t","partition":0,"committedOffset":null,"lag":null,"status":"no_commit"}]},
		{"id":"alpha","protocol":"classic","state":"Stable","members":[{"id":"m","clientId":"c","clientHost":"/10.0.0.1"}],"topics":["t"],"partitions":[{"topic":"t","partition":0,"committedOffset":"5","startOffset":"0","endOffset":"8","lag":"3","status":"ok"}]}]`,
	"/api/clusters/kafka-b/gateway/api/v1/consumer-groups": `[]`,
}

func TestListGroups(t *testing.T) {
	get := &fakeGetJSON{answers: groupReads}
	groups, failures, err := NewListGroups(clusters.NewListAccessibleClusters(get), get).Execute(context.Background(), console.Session{}, "")
	if err != nil || len(failures) != 0 || len(groups) != 2 || groups[0].ID != "alpha" || groups[0].Cluster != "kafka-a" || *groups[0].Partitions[0].Lag != "3" ||
		groups[0].Members[0].ClientHost != "/10.0.0.1" || TotalLag(groups[1]) != (Lag{Kind: "unknown", Partitions: 1}) {
		t.Fatalf("%+v %+v %v", groups, failures, err)
	}
	failing := &fakeGetJSON{answers: groupReads, errors: map[string]error{
		"/api/clusters/kafka-a/gateway/api/v1/consumer-groups": console.Problem{Status: 503, Code: "cluster_unreachable", Title: "Cluster unreachable", Detail: "list groups: timeout"}}}
	groups, failures, err = NewListGroups(clusters.NewListAccessibleClusters(failing), failing).Execute(context.Background(), console.Session{}, "")
	if err != nil || len(groups) != 0 || len(failures) != 1 || failures[0].Error != "list groups: timeout" {
		t.Fatalf("%+v %+v %v", groups, failures, err)
	}
}
