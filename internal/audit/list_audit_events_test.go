package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeGetJSON struct {
	answers map[string]string
	errors  map[string]error
}

func (f *fakeGetJSON) Execute(_ context.Context, _ console.Session, path string, into any) error {
	if err := f.errors[path]; err != nil {
		return err
	}
	answer, ok := f.answers[path]
	if !ok {
		return fmt.Errorf("unexpected path %s", path)
	}
	return json.Unmarshal([]byte(answer), into)
}

func TestListAuditEvents(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{
		"/api/audit?cluster=kafka-a&limit=1": `{"operationId":"op-1","events":[{"sequence":7,"occurredAt":"2026-11-13T21:07:55Z","actorId":"local:admin","action":"backup.restore","outcome":"accepted"}],"nextCursor":"7"}`,
		"/api/audit":                         `{"operationId":"op-2","events":null,"nextCursor":""}`,
	}}
	page, err := NewListAuditEvents(get).Execute(context.Background(), console.Session{}, "cluster=kafka-a&limit=1")
	if err != nil || page.NextCursor != "7" || len(page.Events) != 1 || page.Events[0].Sequence != 7 || page.Events[0].Action != "backup.restore" {
		t.Fatalf("%+v %v", page, err)
	}
	if page, err := NewListAuditEvents(get).Execute(context.Background(), console.Session{}, ""); err != nil || page.Events == nil || len(page.Events) != 0 {
		t.Fatalf("empty: %+v %v", page, err)
	}
	denied := console.Problem{Status: 403, Code: "audit_access_denied", Title: "Audit access denied"}
	get.errors = map[string]error{"/api/audit": denied}
	if _, err := NewListAuditEvents(get).Execute(context.Background(), console.Session{}, ""); !errors.Is(err, denied) {
		t.Fatalf("denied: %v", err)
	}
}
