package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/audit"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeConsoleCSV struct {
	answers map[string]string
	errors  map[string]error
}

func (f *fakeConsoleCSV) Execute(_ context.Context, _ console.Session, path string) ([]byte, error) {
	if err := f.errors[path]; err != nil {
		return nil, err
	}
	return []byte(f.answers[path]), nil
}

func runAuditExport(csv *fakeConsoleCSV, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	reads := &fakeConsoleReads{}
	auditCommand := NewAuditCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, audit.NewListAuditEvents(reads))
	auditCommand.AddCommand(NewAuditExportCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, audit.NewExportAuditEvents(csv)))
	root := NewRootCommand(auditCommand)
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"audit", "export"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestAuditExportCommand(t *testing.T) {
	csvText := "sequence,action\n1,topics.create\n2,topics.delete\n"
	csv := &fakeConsoleCSV{answers: map[string]string{"/api/audit/export?cluster=kafka-a": csvText},
		errors: map[string]error{"/api/audit/export?cluster=kafka-b": console.Problem{Status: 403, Code: "audit_access_denied"}}}
	if out, _, err := runAuditExport(csv, "--cluster", "kafka-a"); err != nil || out != csvText {
		t.Fatalf("stdout: %v %q", err, out)
	}
	path := filepath.Join(t.TempDir(), "audit.csv")
	out, errOut, err := runAuditExport(csv, "--cluster", "kafka-a", "--file", path)
	data, _ := os.ReadFile(path)
	info, _ := os.Stat(path)
	if err != nil || out != "" || errOut != "Wrote 2 events to "+path+".\n" || string(data) != csvText || info.Mode().Perm() != 0o600 {
		t.Fatalf("file: %v %q %q %q", err, out, errOut, data)
	}
	if _, _, err := runAuditExport(csv, "--cluster", "kafka-a", "--file", path); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing: %v", err)
	}
	if _, _, err := runAuditExport(csv, "--cluster", "kafka-b"); err == nil || !strings.HasPrefix(err.Error(), "denied: Audit for a cluster requires ADMIN") {
		t.Fatalf("denied: %v", err)
	}
	if _, _, err := runAuditExport(csv, "--outcome", "maybe"); err == nil || !strings.HasPrefix(err.Error(), "--outcome must be one of") {
		t.Fatalf("filter: %v", err)
	}
}
