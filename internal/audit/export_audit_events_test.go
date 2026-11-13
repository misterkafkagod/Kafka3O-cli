package audit

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeGetCSV struct {
	path string
	body string
	err  error
}

func (f *fakeGetCSV) Execute(_ context.Context, _ console.Session, path string) ([]byte, error) {
	f.path = path
	return []byte(f.body), f.err
}

func TestExportAuditEvents(t *testing.T) {
	csvText := "sequence,reason\n1,\"line one\nline two\"\n2,plain\n"
	get := &fakeGetCSV{body: csvText}
	body, count, err := NewExportAuditEvents(get).Execute(context.Background(), console.Session{}, "cluster=kafka-a")
	if err != nil || string(body) != csvText || count != 2 || get.path != "/api/audit/export?cluster=kafka-a" {
		t.Fatalf("%q %d %v %s", body, count, err, get.path)
	}
	if _, count, err := NewExportAuditEvents(&fakeGetCSV{body: "sequence,reason\n"}).Execute(context.Background(), console.Session{}, ""); err != nil || count != 0 {
		t.Fatalf("header only: %d %v", count, err)
	}
	if _, _, err := NewExportAuditEvents(&fakeGetCSV{body: "a,\"b\n"}).Execute(context.Background(), console.Session{}, ""); !errors.Is(err, console.ErrConsoleUnavailable) {
		t.Fatalf("broken: %v", err)
	}
	denied := console.Problem{Status: 403, Code: "audit_access_denied"}
	if _, _, err := NewExportAuditEvents(&fakeGetCSV{err: denied}).Execute(context.Background(), console.Session{}, ""); !errors.Is(err, denied) {
		t.Fatalf("denied: %v", err)
	}
}
