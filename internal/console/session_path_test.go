package console

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestSessionPath(t *testing.T) {
	path, err := SessionPath(func() (string, error) { return "/home/u/.config", nil })
	if err != nil || path != filepath.Join("/home/u/.config", "kafka3o", "session.json") {
		t.Fatalf("%q %v", path, err)
	}
	if _, err := SessionPath(func() (string, error) { return "", errors.New("$HOME is not defined") }); err == nil {
		t.Fatal("a missing config dir was accepted")
	}
}
