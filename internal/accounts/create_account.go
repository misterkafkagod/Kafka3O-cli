package accounts

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// CreateAccount creates a local account with a temporary password the account must change at first sign-in.
type CreateAccount interface {
	Execute(ctx context.Context, session console.Session, username, temporaryPassword string) (Account, error)
}

type createAccount struct {
	send console.SendJSON
}

func NewCreateAccount(send console.SendJSON) CreateAccount {
	return createAccount{send: send}
}

// Execute requires the created account in the answer.
func (c createAccount) Execute(ctx context.Context, session console.Session, username, temporaryPassword string) (Account, error) {
	body := struct {
		Username          string `json:"username"`
		TemporaryPassword string `json:"temporaryPassword"`
	}{username, temporaryPassword}
	var answer struct {
		Account Account `json:"account"`
	}
	if err := c.send.Execute(ctx, session, http.MethodPost, "/api/admin/local-accounts", body, "", &answer); err != nil {
		return Account{}, err
	}
	if answer.Account.ID == "" || !strings.EqualFold(answer.Account.Username, username) {
		return Account{}, fmt.Errorf("%w: the Console did not confirm creating %s", console.ErrOutcomeUnknown, username)
	}
	return answer.Account, nil
}
