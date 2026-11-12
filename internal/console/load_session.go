package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// LoadSession reads the session file; a missing, unreadable, incomplete or expired session means signed out.
func LoadSession(path string, now func() time.Time) (Session, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Session{}, ErrSignedOut
	}
	if err != nil {
		return Session{}, fmt.Errorf("%w: read %s: %v", ErrSignedOut, path, err)
	}
	var session Session
	if err := json.Unmarshal(data, &session); err != nil || session.ConsoleURL == "" || session.Token == "" || session.CSRF == "" {
		return Session{}, fmt.Errorf("%w: the session file is not valid", ErrSignedOut)
	}
	if !session.ExpiresAt.IsZero() && !now().Before(session.ExpiresAt) {
		return Session{}, fmt.Errorf("%w: the session expired at %s", ErrSignedOut, session.ExpiresAt.Format(time.RFC3339))
	}
	return session, nil
}
