// Package console talks to the secured Kafka3O Console the way the browser does: local sign-in, the session
// cookies, and every Gateway call forwarded through /api/clusters/{id}/gateway (cli-console-session recipe).
package console

import "errors"

var (
	// ErrSignInFailed covers a wrong username or password and a disabled account; the Console does not tell them apart.
	ErrSignInFailed           = errors.New("wrong username or password, or the account is disabled")
	ErrSignInThrottled        = errors.New("too many sign-in attempts; try again later")
	ErrPasswordChangeRequired = errors.New("password change required: change it in the console first")
	ErrSignedOut              = errors.New("signed out; run kafka3o login")
	ErrConsoleUnavailable     = errors.New("the console could not be reached or gave an unexpected answer")
	// ErrOutcomeUnknown means a change was sent but its result is unknown: it may have been applied.
	ErrOutcomeUnknown = errors.New("the change may have been applied; check before retrying")
	// ErrCanceled means the user declined a change, so nothing was sent.
	ErrCanceled = errors.New("canceled; nothing was sent")
)
