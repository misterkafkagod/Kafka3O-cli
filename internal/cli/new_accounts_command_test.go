package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/accounts"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

var accountReads = map[string]string{
	"/api/admin/local-accounts?limit=100": `{"operationId":"o","accounts":[{"id":"a1","username":"admin","enabled":true,"mustChangePassword":false},
		{"id":"a2","username":"kafka3o-cli-test","enabled":false,"mustChangePassword":true}],"nextCursor":""}`,
}

func runAccountsCommand(reads *fakeConsoleReads, args ...string) (string, error) {
	var out bytes.Buffer
	root := NewRootCommand(NewAccountsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, accounts.NewListAccounts(reads)))
	root.SetOut(&out)
	root.SetArgs(append([]string{"accounts"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestAccountsCommand(t *testing.T) {
	out, err := runAccountsCommand(&fakeConsoleReads{answers: accountReads})
	if err != nil || out != "USERNAME          ENABLED  PASSWORD     ID\nadmin             yes      set          a1\nkafka3o-cli-test  no       must change  a2\n" {
		t.Fatalf("%v\n%s", err, out)
	}
	if out, err := runAccountsCommand(&fakeConsoleReads{answers: accountReads}, "-o", "json"); err != nil || !strings.Contains(out, `"mustChangePassword": true`) {
		t.Fatalf("json: %v\n%s", err, out)
	}
	denied := &fakeConsoleReads{errors: map[string]error{"/api/admin/local-accounts?limit=100": console.Problem{Status: 403, Code: "global_admin_required"}}}
	if _, err := runAccountsCommand(denied); err == nil || !strings.HasPrefix(err.Error(), "denied: Only a local global administrator") {
		t.Fatalf("denied: %v", err)
	}
}
