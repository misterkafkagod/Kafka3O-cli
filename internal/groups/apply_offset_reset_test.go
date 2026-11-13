package groups

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestApplyOffsetResetSendsThePreview(t *testing.T) {
	var preview ResetPreview
	_ = json.Unmarshal([]byte(`{"group":"probe","topic":"t","partitions":[{"partition":0,"currentOffset":"5","targetOffset":"66"},{"partition":1,"currentOffset":null,"targetOffset":"3"}]}`), &preview)
	send := &fakeSendJSON{answer: `{"group":"probe","topic":"t","partitions":[{"partition":0,"committedOffset":"66"},{"partition":1,"committedOffset":"3"}]}`}
	applied, err := NewApplyOffsetReset(send).Execute(context.Background(), console.Session{}, "kafka-a", preview, "")
	if err != nil || len(applied.Partitions) != 2 || applied.Partitions[0].CommittedOffset != "66" {
		t.Fatalf("%+v %v", applied, err)
	}
	if send.path != "/api/clusters/kafka-a/gateway/api/v1/consumer-groups/probe/offset-reset" ||
		send.body != `{"topic":"t","partitions":[{"partition":0,"expectedOffset":"5","targetOffset":"66"},{"partition":1,"expectedOffset":null,"targetOffset":"3"}]}` {
		t.Fatalf("%+v", send)
	}
	other := &fakeSendJSON{answer: `{"group":"other","topic":"t","partitions":[]}`}
	if _, err := NewApplyOffsetReset(other).Execute(context.Background(), console.Session{}, "kafka-a", preview, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("%v", err)
	}
}
