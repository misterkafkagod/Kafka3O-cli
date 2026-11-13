package messages

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestSearchMessages(t *testing.T) {
	get := &fakeGetJSON{answer: `{"messages":[{"partition":0,"offset":61,"offsetExact":"61","timestamp":"2026-11-12T04:08:52.928Z","key":null,"value":"Y2hlY2sgdHdv","headers":[]}],"scanned":141,"truncated":false}`}
	result, err := NewSearchMessages(get).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-m10-verify", "check two & more")
	if err != nil || len(result.Messages) != 1 || result.Scanned != 141 || result.Truncated ||
		get.paths[0] != "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-m10-verify/messages/search?query=check+two+%26+more" {
		t.Fatalf("%+v %v %v", result, err, get.paths)
	}
	empty := &fakeGetJSON{answer: `{"scanned":0,"truncated":false}`}
	if result, _ := NewSearchMessages(empty).Execute(context.Background(), console.Session{}, "kafka-a", "t", "x"); result.Messages == nil {
		t.Fatal("nil messages")
	}
}
