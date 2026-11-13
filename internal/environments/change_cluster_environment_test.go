package environments

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestChangeClusterEnvironment(t *testing.T) {
	send := &fakeSendJSON{answer: `{"operationId":"op","policyRevision":6}`}
	result, err := NewChangeClusterEnvironment(send).Execute(context.Background(), console.Session{}, "kafka-a", "NON_PRODUCTION", "PRODUCTION", "CHG-1 go live")
	if err != nil || result.PolicyRevision != 6 || send.method != "PUT" || send.path != "/api/admin/clusters/kafka-a/environment" ||
		send.body != `{"expectedEnvironment":"NON_PRODUCTION","environment":"PRODUCTION","reason":"CHG-1 go live"}` {
		t.Fatalf("%+v %v %+v", result, err, send)
	}
	if _, err := NewChangeClusterEnvironment(&fakeSendJSON{answer: `{}`}).Execute(context.Background(), console.Session{}, "kafka-a", "", "PRODUCTION", "r"); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("missing answer: %v", err)
	}
}
