package cli

import (
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestWriteError(t *testing.T) {
	for _, tt := range []struct {
		err  error
		text string
		exit int
	}{
		{console.Problem{Status: 403, Code: "forbidden", Detail: "Not permitted: change_reason_required"}, "denied: Production changes need a recorded change reason.", ExitUsage},
		{console.Problem{Status: 409, Code: "topic_exists", Title: "Conflict", Detail: `Topic "t" already exists`}, `refused: Topic "t" already exists`, ExitUsage},
		{console.Problem{Status: 400, Code: "invalid_change_reason", Title: "Invalid change reason"}, "refused: Invalid change reason", ExitUsage},
		{console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"}, "kafka-a: Gateway unreachable: The Gateway could not be reached: at least one cluster could not answer", ExitClusterFail},
	} {
		got := writeError("kafka-a", tt.err)
		if got.Error() != tt.text || ExitCode(got) != tt.exit {
			t.Errorf("%v: %q (exit %d)", tt.err, got, ExitCode(got))
		}
	}
	for _, kept := range []error{nil, console.ErrSignedOut, console.ErrCanceled, console.ErrOutcomeUnknown} {
		if got := writeError("kafka-a", kept); !errors.Is(got, kept) {
			t.Errorf("%v changed to %v", kept, got)
		}
	}
}
