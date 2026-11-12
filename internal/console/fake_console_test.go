package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// fakeConsole answers like the secured Console in the cli-console-session discovery.
type fakeConsole struct {
	mu                 sync.Mutex
	password           string
	mustChangePassword bool
	throttled          bool
	noCookies          bool
	signedIn           map[string]string // session token -> its CSRF token, as the Console stores it
	logouts            []*http.Request
	logins             []*http.Request
}

func newFakeConsole(t *testing.T) (*fakeConsole, *httptest.Server) {
	fake := &fakeConsole{password: "s3cret", signedIn: map[string]string{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/session/local", fake.login)
	mux.HandleFunc("POST /api/session/logout", fake.logout)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return fake, server
}

func problem(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "code": code})
}

func (f *fakeConsole) login(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logins = append(f.logins, r)
	var body struct{ Username, Password string }
	_ = json.NewDecoder(r.Body).Decode(&body)
	switch {
	case r.Header.Get("Origin") != "http://"+r.Host:
		problem(w, http.StatusForbidden, "csrf_rejected")
	case len(r.Cookies()) > 0:
		problem(w, http.StatusConflict, "already_signed_in")
	case f.throttled:
		problem(w, http.StatusTooManyRequests, "authentication_throttled")
	case body.Username != "admin" || body.Password != f.password:
		problem(w, http.StatusUnauthorized, "authentication_failed")
	default:
		if !f.noCookies {
			f.signedIn["token-1"] = "csrf-1"
			http.SetCookie(w, &http.Cookie{Name: SessionCookieName, Value: "token-1", Path: "/", HttpOnly: true, Secure: true})
			http.SetCookie(w, &http.Cookie{Name: CSRFCookieName, Value: "csrf-1", Path: "/", Secure: true})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"accountId": "acc-1", "username": "admin", "authSource": "local",
			"mustChangePassword": f.mustChangePassword, "absoluteExpiresAt": "2026-11-12T20:06:33Z"})
	}
}

func (f *fakeConsole) logout(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logouts = append(f.logouts, r)
	token, _ := r.Cookie(SessionCookieName)
	if token == nil || f.signedIn[token.Value] == "" {
		problem(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	switch {
	case r.Header.Get("Origin") != "http://"+r.Host || r.Header.Get(CSRFHeader) != f.signedIn[token.Value]:
		problem(w, http.StatusForbidden, "csrf_rejected")
	default:
		delete(f.signedIn, token.Value)
		w.WriteHeader(http.StatusNoContent)
	}
}
