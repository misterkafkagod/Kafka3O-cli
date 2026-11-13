package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestDeleteTopic(t *testing.T) {
	send := &fakeSendJSON{answer: `{"name":"kafka3o-cli-p112","status":"accepted"}`}
	if err := NewDeleteTopic(send).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-cli-p112", "r"); err != nil {
		t.Fatal(err)
	}
	if send.method != "DELETE" || send.path != "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p112" || send.body != "" || send.reason != "r" {
		t.Fatalf("%+v", send)
	}
	if err := NewDeleteTopic(&fakeSendJSON{answer: `{"name":"kafka3o-cli-p112","status":"pending"}`}).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-cli-p112", ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("unconfirmed: %v", err)
	}
}
