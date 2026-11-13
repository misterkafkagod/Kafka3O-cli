package audit

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListAuditEvents reads one page of audit events the signed-in user may see.
type ListAuditEvents interface {
	Execute(ctx context.Context, session console.Session, query string) (Page, error)
}

type listAuditEvents struct {
	get console.GetJSON
}

func NewListAuditEvents(get console.GetJSON) ListAuditEvents {
	return listAuditEvents{get: get}
}

// Execute takes a query built by BuildAuditQuery.
func (l listAuditEvents) Execute(ctx context.Context, session console.Session, query string) (Page, error) {
	path := "/api/audit"
	if query != "" {
		path += "?" + query
	}
	page := Page{Events: []Event{}}
	if err := l.get.Execute(ctx, session, path, &page); err != nil {
		return Page{}, err
	}
	if page.Events == nil {
		page.Events = []Event{}
	}
	return page, nil
}
