package cli

import (
	"bytes"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

func TestReassignmentsCommand(t *testing.T) {
	reads := &fakeConsoleReads{answers: map[string]string{
		"/api/clusters": whoamiReads["/api/clusters"],
		"/api/clusters/kafka-a/gateway/api/v1/topics/t/reassignments": `[{"partition":0,"replicas":[1,2],"addingReplicas":[2],"removingReplicas":[3]}]`,
		"/api/clusters/kafka-a/gateway/api/v1/topics/u/reassignments": `[]`,
	}}
	run := func(topic string) string {
		var out bytes.Buffer
		root := NewRootCommand(NewReassignmentsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, clusters.NewListAccessibleClusters(reads), topics.NewListReassignments(reads)))
		root.SetOut(&out)
		root.SetArgs([]string{"reassignments", "kafka-a", topic})
		if err := root.Execute(); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	if got := run("t"); got != "PARTITION  REPLICAS  ADDING  REMOVING\n0          1,2       2       3\n" {
		t.Fatalf("%q", got)
	}
	if got := run("u"); got != "No reassignments in progress.\n" {
		t.Fatalf("%q", got)
	}
}
