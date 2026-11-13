package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
)

const producePath = "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-m10-verify/messages"

func runProduce(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: map[string]string{"/api/clusters": whoamiReads["/api/clusters"]}}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewProduceCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		clusters.NewListAccessibleClusters(reads), messages.NewProduceMessage(writes)))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"produce"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestProduceCommand(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{producePath: `{"partition":0,"offset":"66"}`}}
	out, prompt, err := runProduce(writes, "y\n", "kafka-a", "kafka3o-m10-verify", "--value", "hello from the cli", "--key", "cli",
		"--header", "source=cli", "--header", "run=p111")
	if err != nil || out != "Written to partition 0 at offset 66.\n" {
		t.Fatalf("%v %q", err, out)
	}
	if len(writes.sent) != 1 || writes.sent[0].Body != `{"value":"hello from the cli","key":"cli","headers":[{"key":"source","value":"cli"},{"key":"run","value":"p111"}]}` || writes.sent[0].Reason != "" {
		t.Fatalf("%+v", writes.sent)
	}
	for _, want := range []string{"Change on kafka-a (Non-production):", "  Produce to topic kafka3o-m10-verify", "  header source: cli", "  key: cli", "  value: hello from the cli", "Proceed? [y/N] "} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt misses %q:\n%s", want, prompt)
		}
	}
}

func TestProduceCommandCancelAndRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{producePath: `{"partition":0,"offset":"66"}`}}
	if _, _, err := runProduce(writes, "n\n", "kafka-a", "kafka3o-m10-verify", "--value", "x"); !errors.Is(err, console.ErrCanceled) || ExitCode(err) != ExitUsage || len(writes.sent) != 0 {
		t.Fatalf("cancel: %v %d", err, len(writes.sent))
	}
	if _, _, err := runProduce(writes, "", "kafka-a", "kafka3o-m10-verify", "--value", "x", "--header", "novalue", "--yes"); err == nil || len(writes.sent) != 0 {
		t.Fatalf("bad header: %v", err)
	}
	if _, _, err := runProduce(writes, "", "kafka-a", "kafka3o-m10-verify", "--yes"); err == nil || !strings.Contains(err.Error(), "--value is required") {
		t.Fatalf("no value: %v", err)
	}
	if _, _, err := runProduce(writes, "", "kafka-b", "orders", "--value", "x", "--yes"); !errors.Is(err, ErrReasonRequired) || len(writes.sent) != 0 {
		t.Fatalf("production without reason: %v", err)
	}
	if _, _, err := runProduce(writes, "\n", "kafka-b", "orders", "--value", "x"); !errors.Is(err, console.ErrCanceled) || len(writes.sent) != 0 {
		t.Fatalf("blank reason: %v", err)
	}
}

func TestProduceCommandOnProductionWithAReason(t *testing.T) {
	path := "/api/clusters/kafka-b/gateway/api/v1/topics/orders/messages"
	writes := &fakeConsoleWrites{answers: map[string]string{path: `{"partition":1,"offset":"7"}`}}
	if _, _, err := runProduce(writes, "", "kafka-b", "orders", "--value", "x", "--yes", "--reason", "INC-42 replay"); err != nil || writes.sent[0].Reason != "INC-42 replay" {
		t.Fatalf("%v %+v", err, writes.sent)
	}
	denied := &fakeConsoleWrites{errors: map[string]error{path: console.Problem{Status: 403, Code: "forbidden", Detail: "Not permitted: production_admin_required"}}}
	if _, _, err := runProduce(denied, "", "kafka-b", "orders", "--value", "x", "--yes", "--reason", "r"); err == nil || err.Error() != "denied: This is a Production cluster: only an ADMIN for it may change it." {
		t.Fatalf("denied: %v", err)
	}
	unknown := &fakeConsoleWrites{errors: map[string]error{path: console.ErrOutcomeUnknown}}
	if _, _, err := runProduce(unknown, "", "kafka-b", "orders", "--value", "x", "--yes", "--reason", "r"); ExitCode(err) != ExitOutcomeUnknown {
		t.Fatalf("unknown: %v", err)
	}
}
