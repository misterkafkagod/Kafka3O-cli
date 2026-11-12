package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

var topicCommandReads = map[string]string{
	"/api/clusters": whoamiReads["/api/clusters"],
	"/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-m10-verify": `{"name":"kafka3o-m10-verify","partitions":[{"id":0,"leader":1,"replicas":[1],"isr":[1],"startOffset":0,"endOffset":67}],"config":[]}`,
	"/api/clusters/kafka-a/gateway/api/v1/consumer-groups":           `[]`,
	"/api/clusters/kafka-a/gateway/api/v1/backups":                   `{"backups":[]}`,
}

func runTopicCommand(reads *fakeConsoleReads, args ...string) (string, error) {
	var out bytes.Buffer
	root := NewRootCommand(NewTopicCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		topics.NewDescribeTopic(clusters.NewListAccessibleClusters(reads), reads)))
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs(append([]string{"topic"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestTopicCommandTextAndJSON(t *testing.T) {
	out, err := runTopicCommand(&fakeConsoleReads{answers: topicCommandReads}, "kafka-a", "kafka3o-m10-verify")
	if err != nil || !strings.HasPrefix(out, "Topic kafka3o-m10-verify on kafka-a") || !strings.Contains(out, "No backup for this topic.") {
		t.Fatalf("%v\n%s", err, out)
	}
	out, err = runTopicCommand(&fakeConsoleReads{answers: topicCommandReads}, "kafka-a", "kafka3o-m10-verify", "-o", "json")
	var view topics.TopicView
	if err != nil || json.Unmarshal([]byte(out), &view) != nil || view.Partitions[0].EndOffset != 67 || view.Groups.Groups == nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestTopicCommandFailures(t *testing.T) {
	reads := &fakeConsoleReads{answers: topicCommandReads, errors: map[string]error{
		"/api/clusters/kafka-a/gateway/api/v1/topics/nope": console.Problem{Status: 404, Code: "not_found", Detail: `Topic "nope" was not found`},
		"/api/clusters/kafka-b/gateway/api/v1/topics/x":    console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"},
	}}
	if _, err := runTopicCommand(reads, "kafka-a", "nope"); ExitCode(err) != ExitUsage || err.Error() != "topic nope was not found on kafka-a" {
		t.Errorf("not found: %v", err)
	}
	if _, err := runTopicCommand(reads, "nope", "x"); ExitCode(err) != ExitUsage || !strings.Contains(err.Error(), "cluster nope not found") {
		t.Errorf("unknown cluster: %v", err)
	}
	_, err := runTopicCommand(reads, "kafka-b", "x")
	if ExitCode(err) != ExitClusterFail || err.Error() != "kafka-b: Gateway unreachable: The Gateway could not be reached: at least one cluster could not answer" {
		t.Errorf("unreachable: %v", err)
	}
	if _, err := runTopicCommand(reads, "kafka-a"); err == nil || ExitCode(err) != ExitUsage {
		t.Errorf("missing argument: %v", err)
	}
}
