package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestForgetEndedSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	_ = console.SaveSession(path, console.Session{Username: "admin"})
	ForgetEndedSession(path, errors.New("kafka-b: gateway unavailable"))
	if _, err := os.Stat(path); err != nil {
		t.Fatal("another error removed the session")
	}
	ForgetEndedSession(path, fmt.Errorf("list clusters: %w", console.ErrSignedOut))
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("an ended session was kept")
	}
}
