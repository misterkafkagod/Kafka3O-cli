package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

func runPartitions(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: map[string]string{
		"/api/clusters": whoamiReads["/api/clusters"],
		"/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p113": `{"name":"kafka3o-cli-p113","partitions":[{"id":0,"leader":1,"replicas":[1],"isr":[1]}],"config":[]}`,
		"/api/clusters/kafka-a/gateway/api/v1/consumer-groups":         `[]`,
		"/api/clusters/kafka-a/gateway/api/v1/backups":                 `{"backups":[]}`,
	}}
	accessible := clusters.NewListAccessibleClusters(reads)
	deps := PartitionDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, Accessible: accessible,
		Describe: topics.NewDescribeTopic(accessible, reads), Increase: topics.NewIncreasePartitions(writes), Reassign: topics.NewReassignPartition(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewPartitionsCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"partitions"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestPartitionsAdd(t *testing.T) {
	path := "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p113/partitions"
	writes := &fakeConsoleWrites{answers: map[string]string{path: `{"topic":"kafka3o-cli-p113","status":"accepted"}`}}
	out, prompt, err := runPartitions(writes, "y\n", "add", "kafka-a", "kafka3o-cli-p113", "2")
	if err != nil || out != "Topic kafka3o-cli-p113 now has 2 partitions.\n" || !strings.Contains(prompt, "Topic kafka3o-cli-p113: 1 → 2 partitions") || writes.sent[0].Body != `{"count":2}` {
		t.Fatalf("%v %q %q %+v", err, out, prompt, writes.sent)
	}
	for _, total := range []string{"1", "0", "two"} {
		before := len(writes.sent)
		if _, _, err := runPartitions(writes, "", "add", "kafka-a", "kafka3o-cli-p113", total, "--yes"); err == nil || ExitCode(err) != ExitUsage || len(writes.sent) != before {
			t.Errorf("%s: %v", total, err)
		}
	}
}

func TestPartitionsReassign(t *testing.T) {
	path := "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p113/partitions/0/replicas"
	writes := &fakeConsoleWrites{answers: map[string]string{path: `{"topic":"kafka3o-cli-p113","status":"accepted"}`}}
	out, _, err := runPartitions(writes, "", "reassign", "kafka-a", "kafka3o-cli-p113", "0", "1", "--yes")
	if err != nil || !strings.HasPrefix(out, "Kafka accepted moving kafka3o-cli-p113 partition 0 to brokers 1") || writes.sent[0].Body != `{"replicas":[1]}` {
		t.Fatalf("%v %q %+v", err, out, writes.sent)
	}
	for _, bad := range [][]string{{"0", "1,1"}, {"0", "x"}, {"-1", "1"}} {
		before := len(writes.sent)
		if _, _, err := runPartitions(writes, "", "reassign", "kafka-a", "kafka3o-cli-p113", bad[0], bad[1], "--yes"); err == nil || len(writes.sent) != before {
			t.Errorf("%v: %v", bad, err)
		}
	}
}
