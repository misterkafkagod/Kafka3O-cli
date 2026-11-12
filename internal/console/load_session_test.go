package console

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadSession(t *testing.T) {
	dir := t.TempDir()
	expires := time.Date(2026, 11, 12, 20, 0, 0, 0, time.UTC)
	valid := Session{ConsoleURL: "http://localhost:5173", Username: "admin", ExpiresAt: expires, Token: "t", CSRF: "c"}
	path := filepath.Join(dir, "session.json")
	_ = SaveSession(path, valid)
	before := func() time.Time { return expires.Add(-time.Minute) }
	if got, err := LoadSession(path, before); err != nil || got != valid {
		t.Fatalf("valid: %+v %v", got, err)
	}
	if _, err := LoadSession(path, func() time.Time { return expires }); !errors.Is(err, ErrSignedOut) {
		t.Errorf("expired: %v", err)
	}
	if _, err := LoadSession(filepath.Join(dir, "missing.json"), before); !errors.Is(err, ErrSignedOut) {
		t.Errorf("missing: %v", err)
	}
	corrupt := filepath.Join(dir, "corrupt.json")
	_ = os.WriteFile(corrupt, []byte("{not json"), 0o600)
	if _, err := LoadSession(corrupt, before); !errors.Is(err, ErrSignedOut) {
		t.Errorf("corrupt: %v", err)
	}
	incomplete := filepath.Join(dir, "incomplete.json")
	_ = SaveSession(incomplete, Session{ConsoleURL: "http://x", Token: "t"})
	if _, err := LoadSession(incomplete, before); !errors.Is(err, ErrSignedOut) {
		t.Errorf("incomplete: %v", err)
	}
}
