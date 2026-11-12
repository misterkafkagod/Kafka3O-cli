package clusters

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeList struct {
	clusters []AccessibleCluster
	err      error
}

func (f fakeList) Execute(context.Context, console.Session) ([]AccessibleCluster, error) {
	return f.clusters, f.err
}

// slowRead answers the first cluster last, to show the order does not depend on who finishes first.
type slowRead struct {
	mu      sync.Mutex
	active  int
	maximum int
	err     map[string]error
}

func (s *slowRead) Execute(_ context.Context, _ console.Session, cluster AccessibleCluster) (ClusterHealth, error) {
	s.mu.Lock()
	s.active++
	s.maximum = max(s.maximum, s.active)
	s.mu.Unlock()
	if cluster.ID == "first" {
		time.Sleep(50 * time.Millisecond)
	} else {
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	return ClusterHealth{AccessibleCluster: cluster, Status: StatusHealthy}, s.err[cluster.ID]
}

func TestListClusterHealthKeepsTheOrderAndRunsConcurrently(t *testing.T) {
	read := &slowRead{}
	list := fakeList{clusters: []AccessibleCluster{{ID: "first"}, {ID: "second"}, {ID: "third"}}}
	got, err := NewListClusterHealth(list, read).Execute(context.Background(), console.Session{})
	if err != nil || len(got) != 3 || got[0].ID != "first" || got[2].ID != "third" {
		t.Fatalf("%+v %v", got, err)
	}
	if read.maximum < 2 {
		t.Fatalf("the clusters were read one by one (at most %d at a time)", read.maximum)
	}
}

func TestListClusterHealthFailsOnlyForASignedOutSession(t *testing.T) {
	list := fakeList{clusters: []AccessibleCluster{{ID: "first"}, {ID: "second"}}}
	if _, err := NewListClusterHealth(list, &slowRead{err: map[string]error{"second": console.ErrSignedOut}}).Execute(context.Background(), console.Session{}); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("signed out during reads: %v", err)
	}
	if _, err := NewListClusterHealth(fakeList{err: console.ErrSignedOut}, &slowRead{}).Execute(context.Background(), console.Session{}); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("signed out at the list: %v", err)
	}
}
