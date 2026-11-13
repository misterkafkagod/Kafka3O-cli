package cli

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/connect"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

const connectorActionBase = "/api/clusters/kafka-a/gateway/api/v1/connect/connectors/"

func runConnector(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: whoamiReads}
	deps := ConnectorDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		Accessible: clusters.NewListAccessibleClusters(reads), Run: connect.NewRunConnectorAction(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewConnectorCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"connector"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestConnectorCommand(t *testing.T) {
	for _, test := range []struct {
		args             []string
		path, answer     string
		summary, printed string
	}{
		{[]string{"pause", "kafka-a", "kafka3o-verify-paused"}, "kafka3o-verify-paused/pause", `{"connector":"kafka3o-verify-paused","action":"pause"}`,
			`Pause connector "kafka3o-verify-paused"`, "Pause requested for kafka3o-verify-paused.\n"},
		{[]string{"resume", "kafka-a", "kafka3o-verify-paused"}, "kafka3o-verify-paused/resume", `{"connector":"kafka3o-verify-paused","action":"resume"}`,
			`Resume connector "kafka3o-verify-paused"`, "Resume requested for kafka3o-verify-paused.\n"},
		{[]string{"restart", "kafka-a", "kafka3o-verify-running"}, "kafka3o-verify-running/restart", `{"connector":"kafka3o-verify-running","action":"restart"}`,
			`Restart connector "kafka3o-verify-running" and its tasks`, "Restart requested for kafka3o-verify-running.\n"},
		{[]string{"restart-task", "kafka-a", "kafka3o-verify-running", "0"}, "kafka3o-verify-running/tasks/0/restart", `{"connector":"kafka3o-verify-running","action":"restart-task","task":0}`,
			`Restart task 0 of connector "kafka3o-verify-running"`, "Restart requested for task 0 of kafka3o-verify-running.\n"},
	} {
		writes := &fakeConsoleWrites{answers: map[string]string{connectorActionBase + test.path: test.answer}}
		out, prompt, err := runConnector(writes, "y\n", test.args...)
		if err != nil || out != test.printed || !strings.Contains(prompt, "Change on kafka-a (Non-production):\n  "+test.summary+"\nProceed? [y/N] ") ||
			len(writes.sent) != 1 || writes.sent[0].Method != "POST" || writes.sent[0].Body != "" {
			t.Fatalf("%v: %v\n%s\n%s\n%+v", test.args, err, out, prompt, writes.sent)
		}
	}
}

func TestConnectorCommandRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{}
	if _, _, err := runConnector(writes, "n\n", "pause", "kafka-a", "x"); !errors.Is(err, console.ErrCanceled) || len(writes.sent) != 0 {
		t.Fatalf("cancel: %v", err)
	}
	if _, _, err := runConnector(writes, "", "pause", "kafka-b", "x", "--yes"); !errors.Is(err, ErrReasonRequired) || len(writes.sent) != 0 {
		t.Fatalf("production: %v", err)
	}
	for _, task := range []string{"x", "-1", "1.5"} {
		if _, _, err := runConnector(writes, "", "restart-task", "kafka-a", "x", "--yes", "--", task); err == nil || !strings.Contains(err.Error(), "task must be a task id") || len(writes.sent) != 0 {
			t.Fatalf("task %s: %v", task, err)
		}
	}
	missing := &fakeConsoleWrites{errors: map[string]error{connectorActionBase + "x/tasks/9/restart": console.Problem{Status: 404, Code: "not_found", Detail: "the connector or task does not exist"}}}
	if _, _, err := runConnector(missing, "", "restart-task", "kafka-a", "x", "9", "--yes"); err == nil || err.Error() != "task 9 of connector x was not found on kafka-a" || ExitCode(err) != ExitUsage {
		t.Fatalf("not found: %v", err)
	}
	rejected := &fakeConsoleWrites{errors: map[string]error{connectorActionBase + "x/pause": console.Problem{Status: 409, Code: "connect_rejected", Detail: "Kafka Connect rejected the action; refresh and try again"}}}
	if _, _, err := runConnector(rejected, "", "pause", "kafka-a", "x", "--yes"); err == nil || err.Error() != "refused: Kafka Connect rejected the action; refresh and try again" {
		t.Fatalf("rejected: %v", err)
	}
	unknown := &fakeConsoleWrites{errors: map[string]error{connectorActionBase + "x/restart": fmt.Errorf("%w (503 connect_action_unknown)", console.ErrOutcomeUnknown)}}
	if _, _, err := runConnector(unknown, "", "restart", "kafka-a", "x", "--yes"); ExitCode(err) != ExitOutcomeUnknown {
		t.Fatalf("unknown: %v", err)
	}
	withReason := &fakeConsoleWrites{answers: map[string]string{"/api/clusters/kafka-b/gateway/api/v1/connect/connectors/x/pause": `{"connector":"x","action":"pause"}`}}
	if _, _, err := runConnector(withReason, "", "pause", "kafka-b", "x", "--yes", "--reason", "INC-7"); err != nil || withReason.sent[0].Reason != "INC-7" {
		t.Fatalf("reason: %v %+v", err, withReason.sent)
	}
}
