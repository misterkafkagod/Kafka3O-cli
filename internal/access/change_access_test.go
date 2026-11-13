package access

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestChangeRoleGrant(t *testing.T) {
	send := &fakeSendJSON{answer: `{"operationId":"op","policyRevision":13}`}
	result, err := NewChangeRoleGrant(send).Execute(context.Background(), console.Session{}, "a2", "kafka-a", "", "VIEWER")
	if err != nil || result.PolicyRevision != 13 || send.method != "PUT" || send.path != "/api/admin/role-grants" ||
		send.body != `{"identityId":"a2","clusterId":"kafka-a","expectedRole":"","role":"VIEWER"}` {
		t.Fatalf("%+v %v %+v", result, err, send)
	}
	if _, err := NewChangeRoleGrant(&fakeSendJSON{answer: `{}`}).Execute(context.Background(), console.Session{}, "a2", "", "VIEWER", ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("missing answer: %v", err)
	}
}

func TestChangeGroupMapping(t *testing.T) {
	send := &fakeSendJSON{answer: `{"operationId":"op","policyRevision":14}`}
	result, err := NewChangeGroupMapping(send).Execute(context.Background(), console.Session{}, "https://cognito", "ops", "", "OPERATOR", "")
	if err != nil || result.OperationID != "op" || send.path != "/api/admin/group-mappings" ||
		send.body != `{"issuer":"https://cognito","group":"ops","clusterId":"","expectedRole":"OPERATOR","role":""}` {
		t.Fatalf("%+v %v %+v", result, err, send)
	}
	if _, err := NewChangeGroupMapping(&fakeSendJSON{answer: `{"operationId":"op"}`}).Execute(context.Background(), console.Session{}, "i", "g", "", "", "ADMIN"); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("missing revision: %v", err)
	}
}
