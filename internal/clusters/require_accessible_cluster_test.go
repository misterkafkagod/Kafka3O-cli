package clusters

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestRequireAccessibleCluster(t *testing.T) {
	if err := RequireAccessibleCluster(context.Background(), console.Session{}, threeClusters, "second"); err != nil {
		t.Fatal(err)
	}
	var unknown ErrUnknownCluster
	if err := RequireAccessibleCluster(context.Background(), console.Session{}, threeClusters, "nope"); !errors.As(err, &unknown) {
		t.Fatal(err)
	}
	if err := RequireAccessibleCluster(context.Background(), console.Session{}, fakeList{err: console.ErrSignedOut}, "x"); !errors.Is(err, console.ErrSignedOut) {
		t.Fatal(err)
	}
}
