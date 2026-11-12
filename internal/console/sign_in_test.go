package console

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestSignInKeepsTheCookiesAndExpiry(t *testing.T) {
	fake, server := newFakeConsole(t)
	session, err := NewSignIn(http.DefaultClient, NewSignOut(http.DefaultClient)).Execute(context.Background(), server.URL+"/", "admin", "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	want := Session{ConsoleURL: server.URL, Username: "admin", AccountID: "acc-1", ExpiresAt: time.Date(2026, 11, 12, 20, 6, 33, 0, time.UTC), Token: "token-1", CSRF: "csrf-1"}
	if session != want {
		t.Fatalf("session = %+v", session)
	}
	if fake.logins[0].Header.Get("Content-Type") != "application/json" || len(fake.logouts) != 0 {
		t.Fatalf("login request %v, logouts %d", fake.logins[0].Header, len(fake.logouts))
	}
}

func TestSignInFailures(t *testing.T) {
	for name, setup := range map[string]struct {
		configure func(*fakeConsole)
		password  string
		want      error
	}{
		"wrong password":  {func(*fakeConsole) {}, "wrong", ErrSignInFailed},
		"throttled":       {func(f *fakeConsole) { f.throttled = true }, "s3cret", ErrSignInThrottled},
		"no cookies":      {func(f *fakeConsole) { f.noCookies = true }, "s3cret", ErrConsoleUnavailable},
		"password change": {func(f *fakeConsole) { f.mustChangePassword = true }, "s3cret", ErrPasswordChangeRequired},
	} {
		fake, server := newFakeConsole(t)
		setup.configure(fake)
		session, err := NewSignIn(http.DefaultClient, NewSignOut(http.DefaultClient)).Execute(context.Background(), server.URL, "admin", setup.password)
		if !errors.Is(err, setup.want) || session != (Session{}) {
			t.Errorf("%s: %+v %v", name, session, err)
		}
		if name == "password change" && (len(fake.logouts) != 1 || len(fake.signedIn) != 0) {
			t.Errorf("the session that must change its password was not signed out: %d logouts", len(fake.logouts))
		}
	}
}

func TestSignInRefusesAnInvalidURLAndAnUnreachableConsole(t *testing.T) {
	signIn := NewSignIn(http.DefaultClient, NewSignOut(http.DefaultClient))
	if _, err := signIn.Execute(context.Background(), "localhost:5173", "admin", "x"); err == nil {
		t.Error("an invalid URL was accepted")
	}
	if _, err := signIn.Execute(context.Background(), "http://127.0.0.1:1", "admin", "x"); !errors.Is(err, ErrConsoleUnavailable) {
		t.Errorf("unreachable: %v", err)
	}
}
