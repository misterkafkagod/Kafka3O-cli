package clusters

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

var threeClusters = fakeList{clusters: []AccessibleCluster{{ID: "first"}, {ID: "second"}, {ID: "third"}}}

func TestReadAcrossClustersKeepsTheOrderAndFailures(t *testing.T) {
	read := func(_ context.Context, cluster AccessibleCluster) (string, error) {
		switch cluster.ID {
		case "first":
			time.Sleep(30 * time.Millisecond)
		case "second":
			return "", console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"}
		}
		return cluster.ID + "!", nil
	}
	got, failures, err := ReadAcrossClusters(context.Background(), console.Session{}, threeClusters, "", read)
	if err != nil || len(got) != 2 || got[0] != "first!" || got[1] != "third!" {
		t.Fatalf("%v %v", got, err)
	}
	if len(failures) != 1 || failures[0] != (ClusterFailure{Cluster: "second", Status: StatusGatewayUnreachable, Error: "The Gateway could not be reached"}) {
		t.Fatalf("failures %+v", failures)
	}
}

func TestReadAcrossClustersForOneCluster(t *testing.T) {
	calls := 0
	read := func(_ context.Context, cluster AccessibleCluster) (string, error) { calls++; return cluster.ID, nil }
	got, _, err := ReadAcrossClusters(context.Background(), console.Session{}, threeClusters, "third", read)
	if err != nil || len(got) != 1 || got[0] != "third" || calls != 1 {
		t.Fatalf("%v %v %d", got, err, calls)
	}
	var unknown ErrUnknownCluster
	if _, _, err := ReadAcrossClusters(context.Background(), console.Session{}, threeClusters, "nope", read); !errors.As(err, &unknown) {
		t.Fatalf("unknown: %v", err)
	}
}

func TestReadAcrossClustersFailsForASignedOutSession(t *testing.T) {
	read := func(_ context.Context, cluster AccessibleCluster) (string, error) {
		if cluster.ID == "third" {
			return "", console.ErrSignedOut
		}
		return "", nil
	}
	if _, _, err := ReadAcrossClusters(context.Background(), console.Session{}, threeClusters, "", read); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("%v", err)
	}
	if _, _, err := ReadAcrossClusters(context.Background(), console.Session{}, fakeList{err: console.ErrSignedOut}, "", read); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("list: %v", err)
	}
}
