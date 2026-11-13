package messages

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeGetJSON struct {
	answer string
	err    error
	paths  []string
}

func (f *fakeGetJSON) Execute(_ context.Context, _ console.Session, path string, into any) error {
	f.paths = append(f.paths, path)
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(f.answer), into)
}

const twoMessages = `[{"partition":0,"offset":61,"offsetExact":"61","timestamp":"2026-11-12T04:08:52.928Z","key":"ci1i","value":"Y2hlY2sgdHdv","headers":[{"key":"check","value":"cmVzdG9yZS0xMTEy"}]},
	{"partition":3,"offset":21,"offsetExact":"21","timestamp":"2026-11-12T04:08:52.905Z","key":null,"value":null,"headers":[]}]`

func TestReadMessagesLatestAndFromAPosition(t *testing.T) {
	get := &fakeGetJSON{answer: twoMessages}
	messages, err := NewReadMessages(get).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify", nil)
	if err != nil || len(messages) != 2 || string(messages[0].Key) != "r-b" || string(messages[0].Value) != "check two" || messages[1].Key != nil ||
		string(messages[0].Headers[0].Value) != "restore-1112" || get.paths[0] != "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-m10-verify/messages" {
		t.Fatalf("%+v %v %v", messages, err, get.paths)
	}
	offset, timestamp := int64(61), int64(1794456480000)
	_, _ = NewReadMessages(get).Execute(context.Background(), console.Session{}, "kafka-a", "a/b", &Position{Partition: 0, Offset: &offset})
	_, _ = NewReadMessages(get).Execute(context.Background(), console.Session{}, "kafka-a", "t", &Position{Partition: 2, Timestamp: &timestamp})
	if get.paths[1] != "/api/clusters/kafka-a/gateway/api/v1/topics/a%2Fb/messages?offset=61&partition=0" ||
		get.paths[2] != "/api/clusters/kafka-a/gateway/api/v1/topics/t/messages?partition=2&timestamp=1794456480000" {
		t.Fatalf("%v", get.paths)
	}
	if _, err := NewReadMessages(&fakeGetJSON{err: console.ErrSignedOut}).Execute(context.Background(), console.Session{}, "kafka-a", "t", nil); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("%v", err)
	}
}
