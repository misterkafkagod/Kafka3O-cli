package cli

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/access"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

var accessReads = map[string]string{
	"/api/admin/access?limit=100": `{"operationId":"o","policyRevision":12,"nextCursor":"","groupGrantsTruncated":false,
		"identities":[{"id":"a1","source":"local","label":"admin","enabled":true,"grants":[{"clusterId":"","role":"ADMIN"}]},
			{"id":"a2","source":"local","label":"kafka3o-cli-test","enabled":true,"grants":[]},
			{"id":"a3","source":"local","label":"operator-verify","enabled":true,"grants":[{"clusterId":"","role":"VIEWER"},{"clusterId":"kafka-a","role":"OPERATOR"}]}],
		"groupGrants":[{"issuer":"https://cognito","group":"ops","clusterId":"kafka-a","role":"OPERATOR"}]}`,
}

func runAccess(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: accessReads}
	deps := AccessDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, List: access.NewListAccess(reads),
		Grant: access.NewChangeRoleGrant(writes), Map: access.NewChangeGroupMapping(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewAccessCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"access"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestAccessCommandListing(t *testing.T) {
	out, _, err := runAccess(&fakeConsoleWrites{}, "")
	want := []string{
		"Policy revision 12",
		"IDENTITY SOURCE ENABLED GRANTS ID",
		"admin local yes all:ADMIN a1",
		"kafka3o-cli-test local yes none a2",
		"operator-verify local yes all:VIEWER kafka-a:OPERATOR a3",
		"",
		"ISSUER GROUP SCOPE ROLE",
		"https://cognito ops kafka-a OPERATOR",
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	for i := range lines {
		lines[i] = strings.Join(strings.Fields(lines[i]), " ")
	}
	if err != nil || strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%v\n%s", err, out)
	}
	if out, _, err := runAccess(&fakeConsoleWrites{}, "", "-o", "json"); err != nil || !strings.Contains(out, `"policyRevision": 12`) {
		t.Fatalf("json: %v\n%s", err, out)
	}
}

func TestAccessCommandGrant(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{"/api/admin/role-grants": `{"operationId":"op-1","policyRevision":13}`}}
	out, prompt, err := runAccess(writes, "y\n", "grant", "kafka3o-cli-test", "kafka-a", "viewer")
	if err != nil || out != "Changed; policy revision 13 (operation op-1).\n" ||
		writes.sent[0].Body != `{"identityId":"a2","clusterId":"kafka-a","expectedRole":"","role":"VIEWER"}` ||
		!strings.Contains(prompt, "kafka3o-cli-test (local) on cluster kafka-a: none → VIEWER") {
		t.Fatalf("%v\n%s\n%s\n%+v", err, out, prompt, writes.sent)
	}
	removal := &fakeConsoleWrites{answers: writes.answers}
	if _, prompt, err := runAccess(removal, "", "grant", "operator-verify", "kafka-a", "none", "--yes"); err != nil ||
		removal.sent[0].Body != `{"identityId":"a3","clusterId":"kafka-a","expectedRole":"OPERATOR","role":""}` || !strings.Contains(prompt, "OPERATOR → none") {
		t.Fatalf("removal: %v %s %+v", err, prompt, removal.sent)
	}
	same := &fakeConsoleWrites{}
	if out, _, err := runAccess(same, "", "grant", "operator-verify", "all", "VIEWER", "--yes"); err != nil || len(same.sent) != 0 ||
		out != "Nothing to change: operator-verify (local) already has VIEWER on all clusters.\n" {
		t.Fatalf("same: %v %s", err, out)
	}
}

func TestAccessCommandMapGroupAndRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{"/api/admin/group-mappings": `{"operationId":"op-2","policyRevision":14}`}}
	if _, prompt, err := runAccess(writes, "", "map-group", "https://cognito", "ops", "kafka-a", "ADMIN", "--yes"); err != nil ||
		writes.sent[0].Body != `{"issuer":"https://cognito","group":"ops","clusterId":"kafka-a","expectedRole":"OPERATOR","role":"ADMIN"}` ||
		!strings.Contains(prompt, "group ops of https://cognito on cluster kafka-a: OPERATOR → ADMIN") {
		t.Fatalf("map: %v %s %+v", err, prompt, writes.sent)
	}
	canceled := &fakeConsoleWrites{}
	if _, _, err := runAccess(canceled, "n\n", "map-group", "https://cognito", "devs", "all", "VIEWER"); !errors.Is(err, console.ErrCanceled) || len(canceled.sent) != 0 {
		t.Fatalf("cancel: %v", err)
	}
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"grant", "nobody", "kafka-a", "VIEWER", "--yes"}, "no identity has the id or label nobody"},
		{[]string{"grant", "admin", "kafka a", "VIEWER", "--yes"}, "the scope must be all or a cluster id"},
		{[]string{"grant", "admin", "kafka-a", "owner", "--yes"}, "the role must be VIEWER, OPERATOR, ADMIN or none"},
	} {
		if _, _, err := runAccess(canceled, "", test.args...); err == nil || !strings.HasPrefix(err.Error(), test.want) || len(canceled.sent) != 0 {
			t.Fatalf("%v: %v", test.args, err)
		}
	}
	for problem, want := range map[error]string{
		console.Problem{Status: 409, Code: "policy_conflict"}:    "refused: Access changed since it was read. Check it again and retry.",
		console.Problem{Status: 403, Code: "self_access_change"}: "denied: You cannot change your own access. Ask another global administrator.",
	} {
		refused := &fakeConsoleWrites{errors: map[string]error{"/api/admin/role-grants": problem}}
		if _, _, err := runAccess(refused, "", "grant", "admin", "kafka-a", "VIEWER", "--yes"); err == nil || err.Error() != want {
			t.Fatalf("%v: %v", problem, err)
		}
	}
	unknown := &fakeConsoleWrites{errors: map[string]error{"/api/admin/role-grants": fmt.Errorf("%w (503 access_change_outcome_unknown, operation op-3)", console.ErrOutcomeUnknown)}}
	if _, _, err := runAccess(unknown, "", "grant", "kafka3o-cli-test", "kafka-a", "VIEWER", "--yes"); ExitCode(err) != ExitOutcomeUnknown {
		t.Fatalf("unknown: %v", err)
	}
}
