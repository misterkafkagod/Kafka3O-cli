// Package cli holds one Cobra command per file; each command reads its flags and calls one injected action.
package cli

import (
	"errors"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

const (
	ExitOK          = 0
	ExitUsage       = 1
	ExitClusterFail = 2
	ExitNotSignedIn = 3
	// ExitOutcomeUnknown means a change may have been applied; the user must check before retrying.
	ExitOutcomeUnknown = 4
)

// ErrClusterFailed marks a result where at least one cluster could not answer; the others were still shown.
var ErrClusterFailed = errors.New("at least one cluster could not answer")

// ExitCode maps a command's error to the process exit code: 3 for anything about signing in, 4 when a change may
// have been applied, 2 when a cluster failed, 1 for everything else (usage errors, not found, denied, canceled).
func ExitCode(err error) int {
	switch {
	case err == nil:
		return ExitOK
	case errors.Is(err, console.ErrSignInFailed), errors.Is(err, console.ErrSignInThrottled),
		errors.Is(err, console.ErrPasswordChangeRequired), errors.Is(err, console.ErrSignedOut):
		return ExitNotSignedIn
	case errors.Is(err, console.ErrOutcomeUnknown):
		return ExitOutcomeUnknown
	case errors.Is(err, ErrClusterFailed):
		return ExitClusterFail
	default:
		return ExitUsage
	}
}
