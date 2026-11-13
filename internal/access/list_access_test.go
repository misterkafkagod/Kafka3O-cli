package access

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func pagedAccess() *fakeGetJSON {
	return &fakeGetJSON{answers: map[string]string{
		"/api/admin/access?limit=100": `{"operationId":"o1","policyRevision":12,"identities":[{"id":"a1","source":"local","label":"admin","enabled":true,"grants":[{"clusterId":"","role":"ADMIN"}]}],
			"nextCursor":"a1","groupGrants":[{"issuer":"https://cognito","group":"ops","clusterId":"kafka-a","role":"OPERATOR"}],"groupGrantsTruncated":true}`,
		"/api/admin/access?limit=100&after=a1": `{"operationId":"o2","policyRevision":12,"identities":[{"id":"a2","source":"local","label":"kafka3o-cli-test","enabled":true,"grants":[]},
			{"id":"c1","source":"cognito","label":"Admin","enabled":true,"grants":[]}],"nextCursor":"","groupGrants":[{"issuer":"https://cognito","group":"ops","clusterId":"kafka-a","role":"OPERATOR"}],"groupGrantsTruncated":true}`,
	}}
}

func TestListAccess(t *testing.T) {
	listing, err := NewListAccess(pagedAccess()).Execute(context.Background(), console.Session{})
	if err != nil || listing.PolicyRevision != 12 || len(listing.Identities) != 3 || len(listing.GroupGrants) != 1 || !listing.GroupGrantsTruncated ||
		listing.Identities[0].Grants[0].Role != "ADMIN" {
		t.Fatalf("%+v %v", listing, err)
	}
}
