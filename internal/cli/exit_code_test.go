package cli

import (
	"errors"
	"fmt"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestExitCode(t *testing.T) {
	for err, want := range map[error]int{
		nil:                                ExitOK,
		errors.New("unknown flag: --nope"): ExitUsage,
		console.ErrSignInFailed:            ExitNotSignedIn,
		console.ErrSignInThrottled:         ExitNotSignedIn,
		console.ErrPasswordChangeRequired:  ExitNotSignedIn,
		fmt.Errorf("list clusters: %w", console.ErrSignedOut): ExitNotSignedIn,
		fmt.Errorf("kafka-b: %w", ErrClusterFailed):           ExitClusterFail,
		console.ErrConsoleUnavailable:                         ExitUsage,
		fmt.Errorf("produce: %w", console.ErrOutcomeUnknown):  ExitOutcomeUnknown,
		console.ErrCanceled:                                   ExitUsage,
	} {
		if got := ExitCode(err); got != want {
			t.Errorf("%v: %d, want %d", err, got, want)
		}
	}
}
