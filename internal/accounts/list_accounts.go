package accounts

import (
	"context"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListAccounts reads every local account, following the Console's pages.
type ListAccounts interface {
	Execute(ctx context.Context, session console.Session) ([]Account, error)
}

type listAccounts struct {
	get console.GetJSON
}

func NewListAccounts(get console.GetJSON) ListAccounts {
	return listAccounts{get: get}
}

func (l listAccounts) Execute(ctx context.Context, session console.Session) ([]Account, error) {
	all, after := []Account{}, ""
	for {
		path := "/api/admin/local-accounts?limit=100"
		if after != "" {
			path += "&after=" + url.QueryEscape(after)
		}
		var page struct {
			Accounts   []Account `json:"accounts"`
			NextCursor string    `json:"nextCursor"`
		}
		if err := l.get.Execute(ctx, session, path, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Accounts...)
		if page.NextCursor == "" || page.NextCursor == after {
			return all, nil
		}
		after = page.NextCursor
	}
}
