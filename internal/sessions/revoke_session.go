package sessions

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// RevokeSession ends another Console session; it must sign in again.
type RevokeSession interface {
	Execute(ctx context.Context, session console.Session, id string) error
}

type revokeSession struct {
	send console.SendJSON
}

func NewRevokeSession(send console.SendJSON) RevokeSession {
	return revokeSession{send: send}
}

// Execute requires the Console's operation id in the answer.
func (r revokeSession) Execute(ctx context.Context, session console.Session, id string) error {
	var answer struct {
		OperationID string `json:"operationId"`
	}
	if err := r.send.Execute(ctx, session, http.MethodPost, "/api/admin/sessions/"+url.PathEscape(id)+"/revoke", nil, "", &answer); err != nil {
		return err
	}
	if answer.OperationID == "" {
		return fmt.Errorf("%w: the Console did not confirm revoking the session", console.ErrOutcomeUnknown)
	}
	return nil
}
