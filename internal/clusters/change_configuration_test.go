package clusters

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeSendJSON struct {
	method, path, body, reason string
	answer                     string
	err                        error
}

func (f *fakeSendJSON) Execute(_ context.Context, _ console.Session, method, path string, body any, reason string, into any) error {
	raw, _ := json.Marshal(body)
	f.method, f.path, f.body, f.reason = method, path, string(raw), reason
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(f.answer), into)
}

func TestChangeConfiguration(t *testing.T) {
	send := &fakeSendJSON{answer: `{"name":"retention.ms","status":"accepted"}`}
	value := "3600000"
	topic := ConfigurationResource{Kind: "topic", Name: "kafka3o-cli-p113"}
	if err := NewChangeConfiguration(send).Execute(context.Background(), console.Session{}, "kafka-a", topic, ConfigurationChange{Name: "retention.ms", Operation: "set", Value: &value}, ""); err != nil {
		t.Fatal(err)
	}
	if send.method != "POST" || send.path != "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p113/config" || send.body != `{"name":"retention.ms","operation":"set","value":"3600000"}` {
		t.Fatalf("%+v", send)
	}
	_ = NewChangeConfiguration(send).Execute(context.Background(), console.Session{}, "kafka-a", topic, ConfigurationChange{Name: "retention.ms", Operation: "reset"}, "")
	if send.body != `{"name":"retention.ms","operation":"reset","value":null}` {
		t.Fatalf("reset body %s", send.body)
	}
	unconfirmed := &fakeSendJSON{answer: `{"name":"retention.ms","status":"pending"}`}
	if err := NewChangeConfiguration(unconfirmed).Execute(context.Background(), console.Session{}, "kafka-a", topic, ConfigurationChange{Name: "retention.ms", Operation: "reset"}, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("unconfirmed: %v", err)
	}
}
