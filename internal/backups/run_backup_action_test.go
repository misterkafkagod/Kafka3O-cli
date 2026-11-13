package backups

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestRunBackupAction(t *testing.T) {
	base := "/api/clusters/kafka-a/gateway/api/v1/backups/"
	for _, test := range []struct{ kind, method, path string }{
		{ActionPause, "POST", base + "a%2Fb/pause"},
		{ActionResume, "POST", base + "a%2Fb/resume"},
		{ActionDelete, "DELETE", base + "a%2Fb"},
	} {
		send := &fakeSendJSON{answer: `{"topic":"a/b","action":"` + test.kind + `"}`}
		if err := NewRunBackupAction(send).Execute(context.Background(), console.Session{}, "kafka-a", "a/b", test.kind, "INC-4"); err != nil ||
			send.method != test.method || send.path != test.path || send.body != "null" || send.reason != "INC-4" {
			t.Fatalf("%s: %v %+v", test.kind, err, send)
		}
	}
	if err := NewRunBackupAction(&fakeSendJSON{answer: `{"topic":"a/b","action":"pause"}`}).Execute(context.Background(), console.Session{}, "kafka-a", "a/b", ActionDelete, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("echo: %v", err)
	}
}
