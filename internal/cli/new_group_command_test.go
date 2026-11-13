package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
)

func runGroupCommand(reads *fakeConsoleReads, args ...string) (string, error) {
	var out bytes.Buffer
	root := NewRootCommand(NewGroupCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		groups.NewListGroups(clusters.NewListAccessibleClusters(reads), reads)))
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs(append([]string{"group"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestGroupCommand(t *testing.T) {
	reads := &fakeConsoleReads{answers: groupsReads}
	out, err := runGroupCommand(reads, "kafka-a", "connect-kafka3o-backup-e2e")
	if err != nil || !strings.HasPrefix(out, "Group connect-kafka3o-backup-e2e on kafka-a\nState: Empty (classic protocol)") || !strings.Contains(out, "Offsets for kafka3o-m10-verify") {
		t.Fatalf("%v\n%s", err, out)
	}
	out, _ = runGroupCommand(reads, "kafka-a", "connect-kafka3o-verify-running", "-o", "json")
	var group groups.ConsumerGroup
	if json.Unmarshal([]byte(out), &group) != nil || group.ID != "connect-kafka3o-verify-running" || group.Cluster != "kafka-a" || *group.Partitions[0].Lag != "0" {
		t.Fatalf("json:\n%s", out)
	}
}

func TestGroupCommandFailures(t *testing.T) {
	reads := &fakeConsoleReads{answers: groupsReads, errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/consumer-groups": console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"}}}
	if _, err := runGroupCommand(reads, "kafka-a", "nope"); ExitCode(err) != ExitUsage || err.Error() != "group nope was not found on kafka-a" {
		t.Errorf("missing: %v", err)
	}
	if _, err := runGroupCommand(reads, "nope", "g"); ExitCode(err) != ExitUsage {
		t.Errorf("unknown cluster: %v", err)
	}
	if _, err := runGroupCommand(reads, "kafka-b", "g"); ExitCode(err) != ExitClusterFail {
		t.Errorf("unreachable: %v", err)
	}
}
