package accounts

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestFindAccount(t *testing.T) {
	find := NewFindAccount(NewListAccounts(pagedAccounts()))
	if account, err := find.Execute(context.Background(), console.Session{}, "KAFKA3O-CLI-TEST"); err != nil || account.ID != "a2" {
		t.Fatalf("%+v %v", account, err)
	}
	_, err := find.Execute(context.Background(), console.Session{}, "nobody")
	if !errors.As(err, new(ErrAccountNotFound)) || err.Error() != "no local account is named nobody" {
		t.Fatalf("missing: %v", err)
	}
}
