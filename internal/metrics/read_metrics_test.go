package metrics

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
	path   string
}

func (f *fakeGetJSON) Execute(_ context.Context, _ console.Session, path string, into any) error {
	f.path = path
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(f.answer), into)
}

func TestReadMetricsReturnsTheDocumentUnchangedAndTyped(t *testing.T) {
	document := `{"readAt":"2026-11-13T10:00:00.123456789Z","logDirs":{"dirs":[{"broker":1,"dir":"/d","totalBytes":94521434112,"usableBytes":79722250240,"kafkaBytes":397274}]},"partitions":{"topics":6,"partitions":39,"underReplicated":0,"offline":0,"belowMinIsr":0,"unreadableTopics":[]},"largestTopics":{"topics":[]},"endOffsets":{"topics":[]},"extra":{"kept":true}}`
	get := &fakeGetJSON{answer: document}
	raw, typed, err := NewReadMetrics(get).Execute(context.Background(), console.Session{}, "kafka-a")
	if err != nil || string(raw) != document || get.path != "/api/clusters/kafka-a/gateway/api/v1/metrics" {
		t.Fatalf("%v %s %s", err, raw, get.path)
	}
	if typed.ReadAt != "2026-11-13T10:00:00.123456789Z" || typed.LogDirs.Dirs[0].TotalBytes.String() != "94521434112" || typed.Partitions.Partitions.String() != "39" {
		t.Fatalf("%+v", typed)
	}
	if _, _, err := NewReadMetrics(&fakeGetJSON{err: console.ErrSignedOut}).Execute(context.Background(), console.Session{}, "kafka-a"); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("%v", err)
	}
}
