package accounts

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func pagedAccounts() *fakeGetJSON {
	return &fakeGetJSON{answers: map[string]string{
		"/api/admin/local-accounts?limit=100":           `{"operationId":"o1","accounts":[{"id":"a1","username":"admin","enabled":true}],"nextCursor":"c 1"}`,
		"/api/admin/local-accounts?limit=100&after=c+1": `{"operationId":"o2","accounts":[{"id":"a2","username":"kafka3o-cli-test","enabled":false,"mustChangePassword":true}],"nextCursor":""}`,
	}}
}

func TestListAccounts(t *testing.T) {
	all, err := NewListAccounts(pagedAccounts()).Execute(context.Background(), console.Session{})
	if err != nil || len(all) != 2 || all[1].Username != "kafka3o-cli-test" || !all[1].MustChangePassword || all[1].Enabled {
		t.Fatalf("%+v %v", all, err)
	}
}
