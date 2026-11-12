package console

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeleteSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	_ = SaveSession(path, Session{Username: "admin"})
	if err := DeleteSession(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("the file is still there")
	}
	if err := DeleteSession(path); err != nil {
		t.Fatalf("deleting a missing file: %v", err)
	}
}
