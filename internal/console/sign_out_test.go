package console

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestSignOutSendsTheWriteHeaders(t *testing.T) {
	fake, server := newFakeConsole(t)
	fake.signedIn["token-1"] = "csrf-1"
	if err := NewSignOut(http.DefaultClient).Execute(context.Background(), Session{ConsoleURL: server.URL, Token: "token-1", CSRF: "csrf-1"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.signedIn) != 0 {
		t.Fatal("the session is still signed in")
	}
}

func TestSignOutOfAnEndedSessionIsNotAnError(t *testing.T) {
	_, server := newFakeConsole(t)
	if err := NewSignOut(http.DefaultClient).Execute(context.Background(), Session{ConsoleURL: server.URL, Token: "gone", CSRF: "c"}); err != nil {
		t.Fatal(err)
	}
}

func TestSignOutReportsARefusal(t *testing.T) {
	fake, server := newFakeConsole(t)
	fake.signedIn["token-1"] = "csrf-1"
	err := NewSignOut(http.DefaultClient).Execute(context.Background(), Session{ConsoleURL: server.URL, Token: "token-1", CSRF: "wrong"})
	if !errors.Is(err, ErrConsoleUnavailable) || len(fake.signedIn) != 1 {
		t.Fatalf("err = %v", err)
	}
}
