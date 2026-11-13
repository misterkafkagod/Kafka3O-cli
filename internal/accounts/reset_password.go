package accounts

import (
	"context"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ResetPassword gives a local account a new temporary password; the account must change it and its sessions end.
type ResetPassword interface {
	Execute(ctx context.Context, session console.Session, account Account, temporaryPassword string) (ChangeResult, error)
}

type resetPassword struct {
	send console.SendJSON
}

func NewResetPassword(send console.SendJSON) ResetPassword {
	return resetPassword{send: send}
}

func (r resetPassword) Execute(ctx context.Context, session console.Session, account Account, temporaryPassword string) (ChangeResult, error) {
	body := struct {
		TemporaryPassword string `json:"temporaryPassword"`
	}{temporaryPassword}
	var result ChangeResult
	err := r.send.Execute(ctx, session, http.MethodPost, "/api/admin/local-accounts/"+url.PathEscape(account.ID)+"/password-reset", body, "", &result)
	return result, err
}
