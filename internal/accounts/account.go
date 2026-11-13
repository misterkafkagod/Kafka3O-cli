package accounts

import "fmt"

// Account is a local Console account; its password is never part of it.
type Account struct {
	ID                 string `json:"id"`
	Username           string `json:"username"`
	Enabled            bool   `json:"enabled"`
	MustChangePassword bool   `json:"mustChangePassword"`
}

// ChangeResult is the Console's answer to an account change; SignInRequired means the account's sessions ended.
type ChangeResult struct {
	OperationID    string `json:"operationId"`
	SignInRequired bool   `json:"signInRequired"`
}

// ErrAccountNotFound means no local account has this username.
type ErrAccountNotFound struct {
	Username string
}

func (e ErrAccountNotFound) Error() string {
	return fmt.Sprintf("no local account is named %s", e.Username)
}
