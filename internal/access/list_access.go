package access

import (
	"context"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListAccess reads every identity with its grants, and the Cognito group mappings.
type ListAccess interface {
	Execute(ctx context.Context, session console.Session) (Listing, error)
}

type listAccess struct {
	get console.GetJSON
}

func NewListAccess(get console.GetJSON) ListAccess {
	return listAccess{get: get}
}

// Execute follows the identity pages; the group mappings and policy revision come from the first page.
func (l listAccess) Execute(ctx context.Context, session console.Session) (Listing, error) {
	listing, after, first := Listing{Identities: []Identity{}, GroupGrants: []GroupGrant{}}, "", true
	for {
		path := "/api/admin/access?limit=100"
		if after != "" {
			path += "&after=" + url.QueryEscape(after)
		}
		var page struct {
			Listing
			NextCursor string `json:"nextCursor"`
		}
		if err := l.get.Execute(ctx, session, path, &page); err != nil {
			return Listing{}, err
		}
		if first {
			listing.PolicyRevision, listing.GroupGrantsTruncated = page.PolicyRevision, page.GroupGrantsTruncated
			listing.GroupGrants = append(listing.GroupGrants, page.GroupGrants...)
			first = false
		}
		listing.Identities = append(listing.Identities, page.Identities...)
		if page.NextCursor == "" || page.NextCursor == after {
			return listing, nil
		}
		after = page.NextCursor
	}
}
