package cli

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestConfirmChange(t *testing.T) {
	var prompt bytes.Buffer
	if err := ConfirmChange(bufio.NewReader(strings.NewReader("y\n")), &prompt, []string{"Disable local account \"t\""}, false); err != nil ||
		prompt.String() != "Change:\n  Disable local account \"t\"\nProceed? [y/N] " {
		t.Fatalf("%v %q", err, prompt.String())
	}
	for _, answer := range []string{"n\n", "\n", ""} {
		if err := ConfirmChange(bufio.NewReader(strings.NewReader(answer)), &bytes.Buffer{}, nil, false); !errors.Is(err, console.ErrCanceled) {
			t.Errorf("%q: %v", answer, err)
		}
	}
	prompt.Reset()
	if err := ConfirmChange(bufio.NewReader(strings.NewReader("")), &prompt, []string{"x"}, true); err != nil || strings.Contains(prompt.String(), "Proceed") {
		t.Fatalf("yes: %v %q", err, prompt.String())
	}
}
