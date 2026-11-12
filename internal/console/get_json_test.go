package console

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, _ := r.Cookie(SessionCookieName)
		csrf, _ := r.Cookie(CSRFCookieName)
		switch {
		case token == nil || token.Value != "token-1" || csrf == nil || csrf.Value != "csrf-1":
			problem(w, http.StatusUnauthorized, "unauthorized")
		case r.URL.Path == "/api/clusters":
			_, _ = w.Write([]byte(`{"clusters":[{"id":"kafka-a","environment":"NON_PRODUCTION","effectiveRole":"ADMIN"}]}`))
		case r.URL.Path == "/api/admin/access":
			problem(w, http.StatusForbidden, "forbidden")
		default:
			_, _ = w.Write([]byte(`not json`))
		}
	}))
	defer server.Close()
	session := Session{ConsoleURL: server.URL, Token: "token-1", CSRF: "csrf-1"}
	get := NewGetJSON(http.DefaultClient)

	var clusters struct {
		Clusters []struct{ ID, Environment, EffectiveRole string }
	}
	if err := get.Execute(context.Background(), session, "/api/clusters", &clusters); err != nil || clusters.Clusters[0].ID != "kafka-a" {
		t.Fatalf("%+v %v", clusters, err)
	}
	var problemErr Problem
	if err := get.Execute(context.Background(), session, "api/admin/access", &clusters); !errors.As(err, &problemErr) || problemErr.Status != 403 || problemErr.Code != "forbidden" {
		t.Errorf("403: %v", err)
	}
	if err := get.Execute(context.Background(), session, "/other", &clusters); !errors.Is(err, ErrConsoleUnavailable) {
		t.Errorf("not json: %v", err)
	}
	if err := get.Execute(context.Background(), Session{ConsoleURL: server.URL, Token: "old", CSRF: "c"}, "/api/clusters", &clusters); !errors.Is(err, ErrSignedOut) {
		t.Errorf("401: %v", err)
	}
	if err := get.Execute(context.Background(), Session{ConsoleURL: "http://127.0.0.1:1", Token: "t", CSRF: "c"}, "/api/clusters", &clusters); !errors.Is(err, ErrConsoleUnavailable) {
		t.Errorf("unreachable: %v", err)
	}
}
