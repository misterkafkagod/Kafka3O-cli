package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
)

const resetBase = "/api/clusters/kafka-a/gateway/api/v1/consumer-groups/connect-kafka3o-backup-probe/offset-reset"

func runReset(reads *fakeConsoleReads, writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	accessible := clusters.NewListAccessibleClusters(reads)
	deps := ResetDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, Accessible: accessible,
		Preview: groups.NewPreviewOffsetReset(reads), Apply: groups.NewApplyOffsetReset(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewResetOffsetsCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"reset-offsets"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func resetReads() *fakeConsoleReads {
	return &fakeConsoleReads{answers: map[string]string{
		"/api/clusters": whoamiReads["/api/clusters"],
		resetBase + "?target=latest&topic=kafka3o-m10-verify": `{"group":"connect-kafka3o-backup-probe","topic":"kafka3o-m10-verify","partitions":[{"partition":0,"currentOffset":"5","targetOffset":"66"},{"partition":1,"currentOffset":null,"targetOffset":"3"}]}`,
	}}
}

func TestResetOffsetsCommand(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{resetBase: `{"group":"connect-kafka3o-backup-probe","topic":"kafka3o-m10-verify","partitions":[{"partition":0,"committedOffset":"66"},{"partition":1,"committedOffset":"3"}]}`}}
	out, prompt, err := runReset(resetReads(), writes, "y\n", "kafka-a", "connect-kafka3o-backup-probe", "kafka3o-m10-verify", "--to", "latest")
	if err != nil || out != "Committed offsets for group connect-kafka3o-backup-probe on topic kafka3o-m10-verify:\nPARTITION  COMMITTED\n0          66\n1          3\n" {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(prompt, "PARTITION  CURRENT  NEW\n0          5        66\n1          none     3\n") || !strings.Contains(prompt, "Proceed? [y/N]") {
		t.Fatalf("prompt:\n%s", prompt)
	}
	if writes.sent[0].Body != `{"topic":"kafka3o-m10-verify","partitions":[{"partition":0,"expectedOffset":"5","targetOffset":"66"},{"partition":1,"expectedOffset":null,"targetOffset":"3"}]}` {
		t.Fatalf("body %s", writes.sent[0].Body)
	}
}

func TestResetOffsetsRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{}
	if _, _, err := runReset(resetReads(), writes, "n\n", "kafka-a", "connect-kafka3o-backup-probe", "kafka3o-m10-verify", "--to", "latest"); !errors.Is(err, console.ErrCanceled) || len(writes.sent) != 0 {
		t.Fatalf("cancel: %v", err)
	}
	if _, _, err := runReset(resetReads(), writes, "", "kafka-a", "g", "t", "--to", "yesterday", "--yes"); err == nil || ExitCode(err) != ExitUsage {
		t.Fatalf("bad target: %v", err)
	}
	active := resetReads()
	active.errors = map[string]error{"/api/clusters/kafka-a/gateway/api/v1/consumer-groups/busy/offset-reset?target=earliest&topic=t": console.Problem{Status: 409, Code: "group_active", Detail: "Stop the group's consumers first"}}
	if _, _, err := runReset(active, writes, "", "kafka-a", "busy", "t", "--to", "earliest", "--yes"); err == nil || err.Error() != "refused: Stop the group's consumers first" || len(writes.sent) != 0 {
		t.Fatalf("active: %v", err)
	}
	stale := &fakeConsoleWrites{errors: map[string]error{resetBase: console.Problem{Status: 409, Code: "stale_preview", Detail: "Committed offsets or log ranges changed since the preview; preview again."}}}
	if _, _, err := runReset(resetReads(), stale, "", "kafka-a", "connect-kafka3o-backup-probe", "kafka3o-m10-verify", "--to", "latest", "--yes"); err == nil || !strings.Contains(err.Error(), "preview again") {
		t.Fatalf("stale: %v", err)
	}
	unknown := &fakeConsoleWrites{errors: map[string]error{resetBase: console.ErrOutcomeUnknown}}
	if _, _, err := runReset(resetReads(), unknown, "", "kafka-a", "connect-kafka3o-backup-probe", "kafka3o-m10-verify", "--to", "latest", "--yes"); ExitCode(err) != ExitOutcomeUnknown {
		t.Fatalf("unknown: %v", err)
	}
}

func TestResetOffsetsOnProduction(t *testing.T) {
	path := "/api/clusters/kafka-b/gateway/api/v1/consumer-groups/billing/offset-reset"
	reads := resetReads()
	reads.answers[path+"?target=earliest&topic=orders"] = `{"group":"billing","topic":"orders","partitions":[{"partition":0,"currentOffset":"9","targetOffset":"0"}]}`
	writes := &fakeConsoleWrites{}
	if _, _, err := runReset(reads, writes, "", "kafka-b", "billing", "orders", "--to", "earliest", "--yes"); !errors.Is(err, ErrReasonRequired) || len(writes.sent) != 0 {
		t.Fatalf("no reason: %v", err)
	}
	writes.answers = map[string]string{path: `{"group":"billing","topic":"orders","partitions":[{"partition":0,"committedOffset":"0"}]}`}
	if _, _, err := runReset(reads, writes, "", "kafka-b", "billing", "orders", "--to", "earliest", "--yes", "--reason", "INC-42 replay"); err != nil || writes.sent[0].Reason != "INC-42 replay" {
		t.Fatalf("with reason: %v %+v", err, writes.sent)
	}
}
