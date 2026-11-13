package sessions

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeGetJSON struct {
	answers map[string]string
	errors  map[string]error
}

func (f *fakeGetJSON) Execute(_ context.Context, _ console.Session, path string, into any) error {
	if err := f.errors[path]; err != nil {
		return err
	}
	answer, ok := f.answers[path]
	if !ok {
		return fmt.Errorf("unexpected path %s", path)
	}
	return json.Unmarshal([]byte(answer), into)
}

type fakeSendJSON struct {
	method, path, body string
	answer             string
	err                error
}

func (f *fakeSendJSON) Execute(_ context.Context, _ console.Session, method, path string, body any, _ string, into any) error {
	raw, _ := json.Marshal(body)
	f.method, f.path, f.body = method, path, string(raw)
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(f.answer), into)
}
