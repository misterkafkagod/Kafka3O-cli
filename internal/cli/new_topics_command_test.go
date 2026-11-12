package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

var topicsReads = map[string]string{
	"/api/clusters": whoamiReads["/api/clusters"],
	"/api/clusters/kafka-a/gateway/api/v1/topics": `[{"name":"tert","partitions":1,"replicationFactor":1,"internal":false,"approxMessages":2},
		{"name":"kafka3o-m10-verify","partitions":4,"replicationFactor":1,"internal":false,"approxMessages":118},
		{"name":"__consumer_offsets","partitions":50,"replicationFactor":1,"internal":true,"approxMessages":900}]`,
	"/api/clusters/kafka-b/gateway/api/v1/topics": `[{"name":"orders","partitions":3,"replicationFactor":1,"internal":false,"approxMessages":7}]`,
}

func runTopics(reads *fakeConsoleReads, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	list := topics.NewListTopicsAcrossClusters(clusters.NewListAccessibleClusters(reads), topics.NewListClusterTopics(reads))
	root := NewRootCommand(NewTopicsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, list))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"topics"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestTopicsTable(t *testing.T) {
	out, errOut, err := runTopics(&fakeConsoleReads{answers: topicsReads})
	want := "CLUSTER  TOPIC               PARTITIONS  REPLICATION  MESSAGES\n" +
		"kafka-a  kafka3o-m10-verify  4           1            118\n" +
		"kafka-a  tert                1           1            2\n" +
		"kafka-b  orders              3           1            7\n"
	if err != nil || out != want || errOut != "" {
		t.Fatalf("%v %q\n%s\nwant\n%s", err, errOut, out, want)
	}
}

func TestTopicsFilterInternalAndCluster(t *testing.T) {
	out, _, err := runTopics(&fakeConsoleReads{answers: topicsReads}, "--filter", "VERIFY")
	if err != nil || out != "CLUSTER  TOPIC               PARTITIONS  REPLICATION  MESSAGES\nkafka-a  kafka3o-m10-verify  4           1            118\n" {
		t.Fatalf("filter: %v\n%s", err, out)
	}
	out, _, _ = runTopics(&fakeConsoleReads{answers: topicsReads}, "--internal", "--filter", "offsets")
	if out != "CLUSTER  TOPIC               PARTITIONS  REPLICATION  MESSAGES\nkafka-a  __consumer_offsets  50          1            900\n" {
		t.Fatalf("internal:\n%s", out)
	}
	out, _, _ = runTopics(&fakeConsoleReads{answers: topicsReads}, "--cluster", "kafka-b")
	if out != "CLUSTER  TOPIC   PARTITIONS  REPLICATION  MESSAGES\nkafka-b  orders  3           1            7\n" {
		t.Fatalf("cluster:\n%s", out)
	}
	out, _, _ = runTopics(&fakeConsoleReads{answers: topicsReads}, "--filter", "nothing-matches")
	if out != "No topics.\n" {
		t.Fatalf("nothing: %q", out)
	}
	_, _, err = runTopics(&fakeConsoleReads{answers: topicsReads}, "--cluster", "nope")
	var unknown topics.ErrUnknownCluster
	if !errors.As(err, &unknown) || ExitCode(err) != ExitUsage {
		t.Fatalf("unknown cluster: %v", err)
	}
}

func TestTopicsWithAFailingClusterAndJSON(t *testing.T) {
	reads := &fakeConsoleReads{answers: topicsReads, errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/topics": console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"},
	}}
	out, errOut, err := runTopics(reads)
	if ExitCode(err) != ExitClusterFail || errOut != "kafka-b: Gateway unreachable: The Gateway could not be reached\n" || out == "" {
		t.Fatalf("%v %q\n%s", err, errOut, out)
	}
	out, _, err = runTopics(reads, "-o", "json")
	var got topicList
	if ExitCode(err) != ExitClusterFail || json.Unmarshal([]byte(out), &got) != nil || len(got.Topics) != 2 || len(got.Failures) != 1 ||
		got.Failures[0].Status != "gateway_unreachable" || got.Topics[0].Cluster != "kafka-a" {
		t.Fatalf("json: %v\n%s", err, out)
	}
}
