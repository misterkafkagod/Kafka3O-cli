package accounts

import (
	"context"
	"strings"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// FindAccount resolves a username to its local account.
type FindAccount interface {
	Execute(ctx context.Context, session console.Session, username string) (Account, error)
}

type findAccount struct {
	list ListAccounts
}

func NewFindAccount(list ListAccounts) FindAccount {
	return findAccount{list: list}
}

// Execute compares usernames case-insensitively, as the Console stores them lower-cased.
func (f findAccount) Execute(ctx context.Context, session console.Session, username string) (Account, error) {
	all, err := f.list.Execute(ctx, session)
	if err != nil {
		return Account{}, err
	}
	for _, account := range all {
		if strings.EqualFold(account.Username, username) {
			return account, nil
		}
	}
	return Account{}, ErrAccountNotFound{Username: username}
}
