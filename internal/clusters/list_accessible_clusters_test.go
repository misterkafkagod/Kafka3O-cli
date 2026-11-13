package clusters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeGetJSON struct {
	answers map[string]string
	errors  map[string]error
	err     error
	mu      sync.Mutex
	paths   []string
}

func (f *fakeGetJSON) Execute(_ context.Context, _ console.Session, path string, into any) error {
	f.mu.Lock()
	f.paths = append(f.paths, path)
	f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if err := f.errors[path]; err != nil {
		return err
	}
	answer, ok := f.answers[path]
	if !ok {
		return fmt.Errorf("unexpected path %s", path)
	}
	return json.Unmarshal([]byte(answer), into)
}

func TestListAccessibleClusters(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{"/api/clusters": `{"clusters":[{"id":"kafka-a","name":"Kafka A","environment":"NON_PRODUCTION","effectiveRole":"ADMIN"},{"id":"kafka-b","name":"Kafka B","environment":"PRODUCTION","effectiveRole":"VIEWER"}]}`}}
	got, err := NewListAccessibleClusters(get).Execute(context.Background(), console.Session{})
	if err != nil || len(got) != 2 || got[1] != (AccessibleCluster{ID: "kafka-b", Name: "Kafka B", Environment: "PRODUCTION", EffectiveRole: "VIEWER"}) {
		t.Fatalf("%+v %v", got, err)
	}
	empty := &fakeGetJSON{answers: map[string]string{"/api/clusters": `{}`}}
	if got, err := NewListAccessibleClusters(empty).Execute(context.Background(), console.Session{}); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty: %+v %v", got, err)
	}
	if _, err := NewListAccessibleClusters(&fakeGetJSON{err: console.ErrSignedOut}).Execute(context.Background(), console.Session{}); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("signed out: %v", err)
	}
}
