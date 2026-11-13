package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/audit"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func runAuditCommand(reads *fakeConsoleReads, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewAuditCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, audit.NewListAuditEvents(reads)))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"audit"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

const auditPage = `{"operationId":"op-1","nextCursor":"8","events":[
	{"sequence":7,"occurredAt":"2026-11-13T21:07:55Z","operationId":"op-r","actorId":"local:admin","source":"local","clusterId":"kafka-a","action":"backup.restore","resourceKind":"topic","resourceId":"kafka3o-m10-verify","decision":"allowed","outcome":"accepted","reason":""},
	{"sequence":8,"occurredAt":"2026-11-13T21:08:00Z","operationId":"op-s","actorId":"local:admin","source":"local","clusterId":"","action":"console.login","decision":"allowed","outcome":"succeeded","reason":"INC-1 \u001b[2J"}]}`

func TestAuditCommand(t *testing.T) {
	local := time.Local
	time.Local = time.UTC
	defer func() { time.Local = local }()
	reads := &fakeConsoleReads{answers: map[string]string{"/api/audit?cluster=kafka-a&limit=2": auditPage}}
	out, _, err := runAuditCommand(reads, "--cluster", "kafka-a", "--limit", "2")
	want := []string{
		"TIME OPERATION ACTOR SOURCE CLUSTER ACTION RESOURCE DECISION OUTCOME REASON",
		"2026-11-13 21:07:55 op-r local:admin local kafka-a backup.restore topic:kafka3o-m10-verify allowed accepted",
		`2026-11-13 21:08:00 op-s local:admin local - console.login - allowed succeeded INC-1 \x1b[2J`,
		"More events: --after 8",
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	for i := range lines {
		lines[i] = strings.Join(strings.Fields(lines[i]), " ")
	}
	if err != nil || strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%v\n%s", err, out)
	}
	out, _, err = runAuditCommand(reads, "--cluster", "kafka-a", "--limit", "2", "-o", "json")
	if err != nil || !strings.Contains(out, `"nextCursor": "8"`) || !strings.Contains(out, `"action": "backup.restore"`) {
		t.Fatalf("json: %v\n%s", err, out)
	}
}

func TestAuditCommandRefusals(t *testing.T) {
	reads := &fakeConsoleReads{answers: map[string]string{"/api/audit": `{"operationId":"op","events":[],"nextCursor":""}`},
		errors: map[string]error{
			"/api/audit?cluster=kafka-b": console.Problem{Status: 403, Code: "audit_access_denied", Title: "Audit access denied"},
			"/api/audit?actor=x":         console.Problem{Status: 503, Code: "audit_unavailable", Title: "Audit is unavailable"},
		}}
	if out, _, err := runAuditCommand(reads); err != nil || out != "No audit events.\n" {
		t.Fatalf("empty: %v %q", err, out)
	}
	if _, _, err := runAuditCommand(reads, "--cluster", "kafka-b"); err == nil || err.Error() != "denied: Audit for a cluster requires ADMIN on it; all-cluster audit requires global ADMIN." {
		t.Fatalf("denied: %v", err)
	}
	if _, _, err := runAuditCommand(reads, "--actor", "x"); ExitCode(err) != ExitClusterFail {
		t.Fatalf("unavailable: %v", err)
	}
	if _, _, err := runAuditCommand(reads, "--outcome", "maybe"); err == nil || !strings.HasPrefix(err.Error(), "--outcome must be one of") {
		t.Fatalf("outcome: %v", err)
	}
}
