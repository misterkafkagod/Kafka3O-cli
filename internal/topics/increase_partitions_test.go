package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestIncreasePartitions(t *testing.T) {
	send := &fakeSendJSON{answer: `{"topic":"t","status":"accepted"}`}
	if err := NewIncreasePartitions(send).Execute(context.Background(), console.Session{}, "kafka-a", "t", 2, ""); err != nil {
		t.Fatal(err)
	}
	if send.path != "/api/clusters/kafka-a/gateway/api/v1/topics/t/partitions" || send.body != `{"count":2}` {
		t.Fatalf("%+v", send)
	}
	if err := NewIncreasePartitions(&fakeSendJSON{answer: `{"topic":"t","status":"queued"}`}).Execute(context.Background(), console.Session{}, "kafka-a", "t", 2, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("unconfirmed: %v", err)
	}
}
