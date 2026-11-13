package sessions

import (
	"context"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListSessions reads every active Console session, following the Console's pages.
type ListSessions interface {
	Execute(ctx context.Context, session console.Session) ([]Session, error)
}

type listSessions struct {
	get console.GetJSON
}

func NewListSessions(get console.GetJSON) ListSessions {
	return listSessions{get: get}
}

func (l listSessions) Execute(ctx context.Context, session console.Session) ([]Session, error) {
	all, after := []Session{}, ""
	for {
		path := "/api/admin/sessions?limit=100"
		if after != "" {
			path += "&after=" + url.QueryEscape(after)
		}
		var page struct {
			Sessions   []Session `json:"sessions"`
			NextCursor string    `json:"nextCursor"`
		}
		if err := l.get.Execute(ctx, session, path, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Sessions...)
		if page.NextCursor == "" || page.NextCursor == after {
			return all, nil
		}
		after = page.NextCursor
	}
}
