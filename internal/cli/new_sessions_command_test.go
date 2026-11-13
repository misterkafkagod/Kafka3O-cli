package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/sessions"
)

var sessionReads = map[string]string{"/api/admin/sessions?limit=100": `{"operationId":"o","nextCursor":"","sessions":[
	{"id":"s1","accountId":"a1","username":"admin","authSource":"local","createdAt":"2026-11-13T17:00:00Z","idleExpiresAt":"2026-11-13T18:00:00Z","absoluteExpiresAt":"2026-11-14T01:00:00Z","isCurrent":true},
	{"id":"s2","accountId":"a1","username":"admin","authSource":"local","createdAt":"2026-11-13T21:30:00Z","idleExpiresAt":"2026-11-13T22:30:00Z","absoluteExpiresAt":"2026-11-14T05:30:00Z","isCurrent":false}]}`}

func runSessionsCommand(args ...string) (string, error) {
	reads := &fakeConsoleReads{answers: sessionReads}
	var out bytes.Buffer
	root := NewRootCommand(NewSessionsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, sessions.NewListSessions(reads)))
	root.SetOut(&out)
	root.SetArgs(append([]string{"sessions"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestSessionsCommand(t *testing.T) {
	local := time.Local
	time.Local = time.UTC
	defer func() { time.Local = local }()
	out, err := runSessionsCommand()
	want := []string{
		"USER SOURCE CREATED IDLE EXPIRES EXPIRES CURRENT ID",
		"admin local 2026-11-13 17:00 2026-11-13 18:00 2026-11-14 01:00 yes s1",
		"admin local 2026-11-13 21:30 2026-11-13 22:30 2026-11-14 05:30 s2",
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	for i := range lines {
		lines[i] = strings.Join(strings.Fields(lines[i]), " ")
	}
	if err != nil || strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%v\n%s", err, out)
	}
	if out, err := runSessionsCommand("-o", "json"); err != nil || !strings.Contains(out, `"isCurrent": true`) {
		t.Fatalf("json: %v\n%s", err, out)
	}
}
