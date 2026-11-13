package messages

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeSendJSON struct {
	method, path, reason string
	body                 string
	answer               string
	err                  error
}

func (f *fakeSendJSON) Execute(_ context.Context, _ console.Session, method, path string, body any, reason string, into any) error {
	encoded, _ := json.Marshal(body)
	f.method, f.path, f.body, f.reason = method, path, string(encoded), reason
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(f.answer), into)
}

func TestProduceMessage(t *testing.T) {
	send := &fakeSendJSON{answer: `{"partition":2,"offset":"9007199254740993"}`}
	key := "cli"
	written, err := NewProduceMessage(send).Execute(context.Background(), console.Session{}, "kafka-a", "a b", NewMessage{Value: "hello", Key: &key,
		Headers: []TextHeader{{Key: "source", Value: "cli"}, {Key: "run", Value: "p111"}}}, "")
	if err != nil || written != (Written{Partition: 2, Offset: "9007199254740993"}) {
		t.Fatalf("%+v %v", written, err)
	}
	if send.method != "POST" || send.path != "/api/clusters/kafka-a/gateway/api/v1/topics/a%20b/messages" ||
		send.body != `{"value":"hello","key":"cli","headers":[{"key":"source","value":"cli"},{"key":"run","value":"p111"}]}` {
		t.Fatalf("%+v", send)
	}
	_, _ = NewProduceMessage(send).Execute(context.Background(), console.Session{}, "kafka-a", "t", NewMessage{Value: "x"}, "INC-1")
	if send.body != `{"value":"x","headers":[]}` || send.reason != "INC-1" {
		t.Fatalf("no key or headers: %+v", send)
	}
}
