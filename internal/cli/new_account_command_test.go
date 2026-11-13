package cli

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/accounts"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func runAccount(writes *fakeConsoleWrites, password ReadPassword, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: accountReads}
	list := accounts.NewListAccounts(reads)
	deps := AccountDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, ReadPassword: password,
		Find: accounts.NewFindAccount(list), Create: accounts.NewCreateAccount(writes), Reset: accounts.NewResetPassword(writes), SetEnabled: accounts.NewSetAccountEnabled(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewAccountCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"account"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

const createdAccount = `{"operationId":"o","account":{"id":"a3","username":"kafka3o-cli-new","enabled":true,"mustChangePassword":true}}`

func TestAccountCommandCreate(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{"/api/admin/local-accounts": createdAccount}}
	out, prompt, err := runAccount(writes, nil, "temporary-pass-1\ny\n", "create", "kafka3o-cli-new", "--password-stdin")
	if err != nil || out != "Created local account kafka3o-cli-new; it must change its password at first sign-in.\n" ||
		writes.sent[0].Body != `{"username":"kafka3o-cli-new","temporaryPassword":"temporary-pass-1"}` || strings.Contains(prompt+out, "temporary-pass-1") ||
		!strings.Contains(prompt, "Change:\n  Create local account \"kafka3o-cli-new\"") {
		t.Fatalf("%v\n%s\n%s\n%+v", err, out, prompt, writes.sent)
	}
	hidden := &fakeConsoleWrites{answers: writes.answers}
	if _, _, err := runAccount(hidden, &passwordSequence{passwords: []string{"hidden-password-1", "hidden-password-1"}}, "", "create", "kafka3o-cli-new", "--yes"); err != nil ||
		hidden.sent[0].Body != `{"username":"kafka3o-cli-new","temporaryPassword":"hidden-password-1"}` {
		t.Fatalf("hidden: %v %+v", err, hidden.sent)
	}
}

func TestAccountCommandChanges(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{
		"/api/admin/local-accounts/a2/password-reset": `{"operationId":"o","signInRequired":true}`,
		"/api/admin/local-accounts/a2/enabled":        `{"operationId":"o","enabled":true,"signInRequired":false}`,
	}}
	if out, _, err := runAccount(writes, nil, "new-temporary-12\ny\n", "reset-password", "KAFKA3O-cli-test", "--password-stdin"); err != nil ||
		out != "Reset the password of kafka3o-cli-test; it must change it at next sign-in.\n" || writes.sent[0].Body != `{"temporaryPassword":"new-temporary-12"}` {
		t.Fatalf("reset: %v %s", err, out)
	}
	if out, prompt, err := runAccount(writes, nil, "y\n", "enable", "kafka3o-cli-test"); err != nil || out != "Enabled kafka3o-cli-test.\n" ||
		writes.sent[1].Method != "PUT" || writes.sent[1].Body != `{"enabled":true}` || !strings.Contains(prompt, `Enable local account "kafka3o-cli-test"`) {
		t.Fatalf("enable: %v %s %s", err, out, prompt)
	}
	disable := &fakeConsoleWrites{answers: map[string]string{"/api/admin/local-accounts/a2/enabled": `{"operationId":"o","enabled":false,"signInRequired":true}`}}
	if out, _, err := runAccount(disable, nil, "", "disable", "kafka3o-cli-test", "--yes"); err != nil || out != "Disabled kafka3o-cli-test; its sessions ended.\n" {
		t.Fatalf("disable: %v %s", err, out)
	}
}

func TestAccountCommandRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{}
	for _, test := range []struct {
		stdin string
		args  []string
		want  string
	}{
		{"temporary-pass-1\n", []string{"create", "--password-stdin", "--yes", "--", "-bad"}, `invalid username "-bad"`},
		{"short\n", []string{"create", "kafka3o-cli-new", "--password-stdin", "--yes"}, "the temporary password needs at least 12 characters"},
		{"", []string{"disable", "nobody", "--yes"}, "no local account is named nobody"},
	} {
		if _, _, err := runAccount(writes, nil, test.stdin, test.args...); err == nil || !strings.HasPrefix(err.Error(), test.want) || len(writes.sent) != 0 {
			t.Fatalf("%v: %v", test.args, err)
		}
	}
	if _, _, err := runAccount(writes, &passwordSequence{passwords: []string{"hidden-password-1", "hidden-password-2"}}, "", "create", "kafka3o-cli-new", "--yes"); err == nil || err.Error() != "the two passwords do not match" {
		t.Fatalf("mismatch: %v", err)
	}
	if _, _, err := runAccount(writes, nil, "temporary-pass-1\nn\n", "create", "kafka3o-cli-new", "--password-stdin"); !errors.Is(err, console.ErrCanceled) || len(writes.sent) != 0 {
		t.Fatalf("cancel: %v", err)
	}
	duplicate := &fakeConsoleWrites{errors: map[string]error{"/api/admin/local-accounts": console.Problem{Status: 409, Code: "username_in_use", Title: "Local username is already in use"}}}
	if _, _, err := runAccount(duplicate, nil, "temporary-pass-1\n", "create", "admin", "--password-stdin", "--yes"); err == nil || err.Error() != "refused: Local username is already in use" {
		t.Fatalf("duplicate: %v", err)
	}
	last := &fakeConsoleWrites{errors: map[string]error{"/api/admin/local-accounts/a1/enabled": console.Problem{Status: 409, Code: "last_local_global_admin"}}}
	if _, _, err := runAccount(last, nil, "", "disable", "admin", "--yes"); err == nil || err.Error() != "refused: The last enabled local global administrator cannot be removed." {
		t.Fatalf("last admin: %v", err)
	}
	unknown := &fakeConsoleWrites{errors: map[string]error{"/api/admin/local-accounts/a2/enabled": fmt.Errorf("%w (503 account_state_outcome_unknown, operation op-9)", console.ErrOutcomeUnknown)}}
	if _, _, err := runAccount(unknown, nil, "", "disable", "kafka3o-cli-test", "--yes"); ExitCode(err) != ExitOutcomeUnknown || !strings.Contains(err.Error(), "op-9") {
		t.Fatalf("unknown: %v", err)
	}
}
