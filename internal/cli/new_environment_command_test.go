package cli

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/environments"
)

func runEnvironment(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: registeredReads}
	deps := EnvironmentDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		List: environments.NewListRegisteredClusters(reads), Change: environments.NewChangeClusterEnvironment(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewEnvironmentCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"environment", "set"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

const environmentPath = "/api/admin/clusters/kafka-a/environment"

func TestEnvironmentCommand(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{environmentPath: `{"operationId":"op-1","policyRevision":6}`}}
	out, prompt, err := runEnvironment(writes, "CHG-1 go live\ny\n", "kafka-a", "production")
	if err != nil || out != "Classified kafka-a as Production; policy revision 6 (operation op-1).\n" ||
		writes.sent[0].Body != `{"expectedEnvironment":"NON_PRODUCTION","environment":"PRODUCTION","reason":"CHG-1 go live"}` ||
		!strings.Contains(prompt, "Change reason (required): Change:\n  Classify Kafka A (kafka-a): Non-production → Production\n  reason: CHG-1 go live\nProceed? [y/N] ") {
		t.Fatalf("%v\n%s\n%s\n%+v", err, out, prompt, writes.sent)
	}
	same := &fakeConsoleWrites{}
	if out, _, err := runEnvironment(same, "", "kafka-b", "production", "--yes", "--reason", "x"); err != nil || len(same.sent) != 0 ||
		out != "Nothing to change: Kafka B (kafka-b) is already Production.\n" {
		t.Fatalf("same: %v %s", err, out)
	}
	unclassified := &fakeConsoleWrites{answers: map[string]string{"/api/admin/clusters/kafka-c/environment": `{"operationId":"op-2","policyRevision":7}`}}
	if _, prompt, err := runEnvironment(unclassified, "", "kafka-c", "non-production", "--yes", "--reason", "r"); err != nil || !strings.Contains(prompt, "Unclassified → Non-production") ||
		unclassified.sent[0].Body != `{"expectedEnvironment":"","environment":"NON_PRODUCTION","reason":"r"}` {
		t.Fatalf("unclassified: %v %s", err, prompt)
	}
}

func TestEnvironmentCommandRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{}
	for _, test := range []struct {
		stdin string
		args  []string
		want  error
		text  string
	}{
		{"", []string{"kafka-a", "production", "--yes"}, nil, "a change reason is required to reclassify a cluster: add --reason"},
		{"\n", []string{"kafka-a", "production"}, console.ErrCanceled, ""},
		{"", []string{"kafka-a", "production", "--reason", "P125 demo"}, console.ErrCanceled, ""},
		{"", []string{"kafka-z", "production", "--yes", "--reason", "r"}, nil, "cluster kafka-z is not registered"},
		{"", []string{"kafka-a", "prod", "--yes", "--reason", "r"}, nil, `the environment must be production or non-production, not "prod"`},
	} {
		_, _, err := runEnvironment(writes, test.stdin, test.args...)
		if (test.want != nil && !errors.Is(err, test.want)) || (test.text != "" && (err == nil || err.Error() != test.text)) || len(writes.sent) != 0 {
			t.Fatalf("%v: %v", test.args, err)
		}
	}
	conflict := &fakeConsoleWrites{errors: map[string]error{environmentPath: console.Problem{Status: 409, Code: "policy_conflict"}}}
	if _, _, err := runEnvironment(conflict, "", "kafka-a", "production", "--yes", "--reason", "r"); err == nil || !strings.HasPrefix(err.Error(), "refused: Access changed since it was read") {
		t.Fatalf("conflict: %v", err)
	}
	unknown := &fakeConsoleWrites{errors: map[string]error{environmentPath: fmt.Errorf("%w (503 access_change_outcome_unknown)", console.ErrOutcomeUnknown)}}
	if _, _, err := runEnvironment(unknown, "", "kafka-a", "production", "--yes", "--reason", "r"); ExitCode(err) != ExitOutcomeUnknown {
		t.Fatalf("unknown: %v", err)
	}
}
