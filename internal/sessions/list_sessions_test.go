package sessions

import (
	"context"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestListSessions(t *testing.T) {
	get := &fakeGetJSON{answers: map[string]string{
		"/api/admin/sessions?limit=100":          `{"operationId":"o1","sessions":[{"id":"s1","username":"admin","authSource":"local","isCurrent":true}],"nextCursor":"s1"}`,
		"/api/admin/sessions?limit=100&after=s1": `{"operationId":"o2","sessions":[{"id":"s2","username":"admin","authSource":"local","isCurrent":false}],"nextCursor":""}`,
	}}
	all, err := NewListSessions(get).Execute(context.Background(), console.Session{})
	if err != nil || len(all) != 2 || !all[0].IsCurrent || all[1].ID != "s2" {
		t.Fatalf("%+v %v", all, err)
	}
}
