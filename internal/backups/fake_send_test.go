package backups

import (
	"context"
	"encoding/json"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeSendJSON struct {
	method, path, body, reason string
	answer                     string
	err                        error
}

func (f *fakeSendJSON) Execute(_ context.Context, _ console.Session, method, path string, body any, reason string, into any) error {
	raw, _ := json.Marshal(body)
	f.method, f.path, f.body, f.reason = method, path, string(raw), reason
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(f.answer), into)
}
