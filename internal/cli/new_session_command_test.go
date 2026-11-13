package cli

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/sessions"
)

func runSession(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: sessionReads}
	deps := SessionAdminDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		List: sessions.NewListSessions(reads), Revoke: sessions.NewRevokeSession(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewSessionCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"session", "revoke"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestSessionCommand(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{"/api/admin/sessions/s2/revoke": `{"operationId":"op","signInRequired":false}`}}
	out, prompt, err := runSession(writes, "y\n", "s2")
	if err != nil || out != "Revoked; that session must sign in again.\n" || writes.sent[0].Method != "POST" ||
		!strings.Contains(prompt, "Revoke the local session of admin created 2026-11-13T21:30:00Z (it must sign in again)") {
		t.Fatalf("%v\n%s\n%s", err, out, prompt)
	}
}

func TestSessionCommandRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{}
	if _, _, err := runSession(writes, "", "s1", "--yes"); err == nil || err.Error() != "this is your current session; use kafka3o logout" || len(writes.sent) != 0 {
		t.Fatalf("current: %v", err)
	}
	if _, _, err := runSession(writes, "", "s9", "--yes"); err == nil || err.Error() != "no active session has the id s9" || len(writes.sent) != 0 {
		t.Fatalf("unknown: %v", err)
	}
	if _, _, err := runSession(writes, "n\n", "s2"); !errors.Is(err, console.ErrCanceled) || len(writes.sent) != 0 {
		t.Fatalf("cancel: %v", err)
	}
	unknown := &fakeConsoleWrites{errors: map[string]error{"/api/admin/sessions/s2/revoke": fmt.Errorf("%w (503 session_revoke_outcome_unknown)", console.ErrOutcomeUnknown)}}
	if _, _, err := runSession(unknown, "", "s2", "--yes"); ExitCode(err) != ExitOutcomeUnknown {
		t.Fatalf("outcome: %v", err)
	}
	gone := &fakeConsoleWrites{errors: map[string]error{"/api/admin/sessions/s2/revoke": console.Problem{Status: 404, Code: "session_unavailable", Title: "Session is unavailable or expired"}}}
	if _, _, err := runSession(gone, "", "s2", "--yes"); err == nil || err.Error() != "not found: Session is unavailable or expired" {
		t.Fatalf("gone: %v", err)
	}
}
