package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestCreateTopic(t *testing.T) {
	send := &fakeSendJSON{answer: `{"name":"kafka3o-cli-p112","partitions":1,"replicationFactor":1}`}
	topic := NewTopic{Name: "kafka3o-cli-p112", Partitions: 1, ReplicationFactor: 1}
	if err := NewCreateTopic(send).Execute(context.Background(), console.Session{}, "kafka-a", topic, ""); err != nil {
		t.Fatal(err)
	}
	if send.method != "POST" || send.path != "/api/clusters/kafka-a/gateway/api/v1/topics" || send.body != `{"name":"kafka3o-cli-p112","partitions":1,"replicationFactor":1}` {
		t.Fatalf("%+v", send)
	}
	wrong := &fakeSendJSON{answer: `{"name":"other","partitions":1,"replicationFactor":1}`}
	if err := NewCreateTopic(wrong).Execute(context.Background(), console.Session{}, "kafka-a", topic, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("wrong echo: %v", err)
	}
}
