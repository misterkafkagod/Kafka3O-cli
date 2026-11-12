package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// fakeConsoleReads answers Console paths with fixed JSON, or an error per path.
type fakeConsoleReads struct {
	answers map[string]string
	errors  map[string]error
}

func (f *fakeConsoleReads) Execute(_ context.Context, _ console.Session, path string, into any) error {
	if err := f.errors[path]; err != nil {
		return err
	}
	answer, ok := f.answers[path]
	if !ok {
		return fmt.Errorf("unexpected path %s", path)
	}
	return json.Unmarshal([]byte(answer), into)
}

var signedIn = console.Session{ConsoleURL: "http://localhost:5173", Username: "admin", Token: "secret-session-token", CSRF: "secret-csrf-token"}

func loaded(session console.Session, err error) func() (console.Session, error) {
	return func() (console.Session, error) { return session, err }
}
