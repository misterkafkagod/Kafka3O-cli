package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// fakeConsoleWrites records changes and answers them with fixed JSON or an error per path.
type fakeConsoleWrites struct {
	answers map[string]string
	errors  map[string]error
	sent    []sentWrite
}

type sentWrite struct {
	Method, Path, Body, Reason string
}

func (f *fakeConsoleWrites) Execute(_ context.Context, _ console.Session, method, path string, body any, reason string, into any) error {
	encoded := ""
	if body != nil {
		raw, _ := json.Marshal(body)
		encoded = string(raw)
	}
	f.sent = append(f.sent, sentWrite{method, path, encoded, reason})
	if err := f.errors[path]; err != nil {
		return err
	}
	answer, ok := f.answers[path]
	if !ok {
		return fmt.Errorf("unexpected write %s %s", method, path)
	}
	if into == nil || answer == "" {
		return nil
	}
	return json.Unmarshal([]byte(answer), into)
}
