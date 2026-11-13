package accounts

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// SetAccountEnabled enables or disables a local account; disabling ends its sessions.
type SetAccountEnabled interface {
	Execute(ctx context.Context, session console.Session, account Account, enabled bool) (ChangeResult, error)
}

type setAccountEnabled struct {
	send console.SendJSON
}

func NewSetAccountEnabled(send console.SendJSON) SetAccountEnabled {
	return setAccountEnabled{send: send}
}

// Execute requires the Console's echo of the new state.
func (s setAccountEnabled) Execute(ctx context.Context, session console.Session, account Account, enabled bool) (ChangeResult, error) {
	body := struct {
		Enabled bool `json:"enabled"`
	}{enabled}
	var answer struct {
		ChangeResult
		Enabled *bool `json:"enabled"`
	}
	if err := s.send.Execute(ctx, session, http.MethodPut, "/api/admin/local-accounts/"+url.PathEscape(account.ID)+"/enabled", body, "", &answer); err != nil {
		return ChangeResult{}, err
	}
	if answer.Enabled == nil || *answer.Enabled != enabled {
		return ChangeResult{}, fmt.Errorf("%w: the Console did not confirm the new state of %s", console.ErrOutcomeUnknown, account.Username)
	}
	return answer.ChangeResult, nil
}
