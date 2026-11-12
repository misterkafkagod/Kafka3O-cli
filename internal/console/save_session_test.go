package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSaveSessionWritesAPrivateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kafka3o", "session.json")
	session := Session{ConsoleURL: "http://localhost:5173", Username: "admin", ExpiresAt: time.Date(2026, 11, 12, 20, 6, 33, 0, time.UTC), Token: "t", CSRF: "c"}
	if err := SaveSession(path, session); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %v %v", info.Mode().Perm(), err)
	}
	if dir, _ := os.Stat(filepath.Dir(path)); dir.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode = %v", dir.Mode().Perm())
	}
	var read Session
	data, _ := os.ReadFile(path)
	if json.Unmarshal(data, &read) != nil || read != session {
		t.Fatalf("read back %+v", read)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temporary files left behind: %v", entries)
	}
}

func TestSaveSessionReplacesAnOldFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	_ = SaveSession(path, Session{Username: "old"})
	if err := SaveSession(path, Session{Username: "new"}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	var read Session
	_ = json.Unmarshal(data, &read)
	if read.Username != "new" {
		t.Fatalf("username = %q", read.Username)
	}
}
