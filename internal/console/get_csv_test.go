package console

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetCSV(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, _ := r.Cookie(SessionCookieName)
		csrf, _ := r.Cookie(CSRFCookieName)
		switch {
		case token == nil || token.Value != "token-1" || csrf == nil || csrf.Value != "csrf-1":
			problem(w, http.StatusUnauthorized, "unauthorized")
		case r.URL.Path == "/api/audit/export" && r.URL.RawQuery == "cluster=kafka-a" && r.Header.Get("Accept") == "text/csv, application/problem+json":
			w.Header().Set("Content-Type", "text/csv; charset=utf-8")
			_, _ = w.Write([]byte("sequence,action\n1,topics.create\n"))
		case r.URL.Path == "/api/audit/export":
			problem(w, http.StatusForbidden, "audit_access_denied")
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	session := Session{ConsoleURL: server.URL, Token: "token-1", CSRF: "csrf-1"}
	get := NewGetCSV(http.DefaultClient)
	if body, err := get.Execute(context.Background(), session, "/api/audit/export?cluster=kafka-a"); err != nil || string(body) != "sequence,action\n1,topics.create\n" {
		t.Fatalf("%q %v", body, err)
	}
	var problemErr Problem
	if _, err := get.Execute(context.Background(), session, "/api/audit/export?cluster=kafka-b"); !errors.As(err, &problemErr) || problemErr.Status != 403 || problemErr.Code != "audit_access_denied" {
		t.Errorf("403: %v", err)
	}
	if _, err := get.Execute(context.Background(), session, "/other"); !errors.Is(err, ErrConsoleUnavailable) {
		t.Errorf("not csv: %v", err)
	}
	if _, err := get.Execute(context.Background(), Session{ConsoleURL: server.URL, Token: "old", CSRF: "c"}, "/api/audit/export"); !errors.Is(err, ErrSignedOut) {
		t.Errorf("401: %v", err)
	}
}
