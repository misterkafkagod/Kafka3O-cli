package console

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// SaveSession writes the session file atomically: a 0700 directory, a 0600 temporary file renamed into place, so
// no other user can read the cookies and a crash never leaves half a file.
func SaveSession(path string, session Session) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create the session directory: %w", err)
	}
	data, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".session-*.json")
	if err != nil {
		return fmt.Errorf("write the session file: %w", err)
	}
	defer os.Remove(temporary.Name())
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("write the session file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return fmt.Errorf("write the session file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("write the session file: %w", err)
	}
	if err := os.Rename(temporary.Name(), path); err != nil {
		return fmt.Errorf("write the session file: %w", err)
	}
	return nil
}
