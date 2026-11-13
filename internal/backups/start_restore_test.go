package backups

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestStartRestore(t *testing.T) {
	answer := `{"topic":"kafka3o-m10-verify","restoreId":"m3x1","objects":13,"skipped":2}`
	send := &fakeSendJSON{answer: answer}
	started, err := NewStartRestore(send).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify", nil, "")
	if err != nil || send.method != "POST" || send.path != "/api/clusters/kafka-a/gateway/api/v1/backups/kafka3o-m10-verify/restore" || send.body != "null" ||
		started != (Restore{Topic: "kafka3o-m10-verify", RestoreID: "m3x1", Objects: 13, Skipped: 2}) {
		t.Fatalf("%+v %v %+v", started, err, send)
	}
	selection, _ := ParseRestoreSelection("", "", "1", true)
	send = &fakeSendJSON{answer: answer}
	if _, err := NewStartRestore(send).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify", selection, "INC-5"); err != nil ||
		send.body != `{"partitions":[1]}` || send.reason != "INC-5" {
		t.Fatalf("%v %+v", err, send)
	}
	for _, wrong := range []string{`{"topic":"other","restoreId":"x"}`, `{"topic":"kafka3o-m10-verify"}`} {
		if _, err := NewStartRestore(&fakeSendJSON{answer: wrong}).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify", nil, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
			t.Fatalf("%s: %v", wrong, err)
		}
	}
}
