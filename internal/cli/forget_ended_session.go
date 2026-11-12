package cli

import (
	"errors"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ForgetEndedSession deletes the session file after any command that found the session over (expired, signed out
// elsewhere or revoked), so the next command says "not signed in" straight away. Other errors keep the file.
func ForgetEndedSession(sessionPath string, err error) {
	if errors.Is(err, console.ErrSignedOut) {
		_ = console.DeleteSession(sessionPath)
	}
}
