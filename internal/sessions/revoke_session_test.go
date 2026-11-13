package sessions

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestRevokeSession(t *testing.T) {
	send := &fakeSendJSON{answer: `{"operationId":"op","signInRequired":false}`}
	if err := NewRevokeSession(send).Execute(context.Background(), console.Session{}, "s/2"); err != nil || send.method != "POST" ||
		send.path != "/api/admin/sessions/s%2F2/revoke" || send.body != "null" {
		t.Fatalf("%v %+v", err, send)
	}
	if err := NewRevokeSession(&fakeSendJSON{answer: `{}`}).Execute(context.Background(), console.Session{}, "s2"); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("missing answer: %v", err)
	}
}
