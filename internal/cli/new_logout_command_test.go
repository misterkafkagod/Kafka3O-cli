package cli

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeSignOut struct {
	err   error
	calls int
}

func (f *fakeSignOut) Execute(context.Context, console.Session) error {
	f.calls++
	return f.err
}

func runLogout(load func() (console.Session, error), signOut *fakeSignOut) (string, int, error) {
	var out bytes.Buffer
	deleted := 0
	root := NewRootCommand(NewLogoutCommand(load, signOut, func() error { deleted++; return nil }))
	root.SetOut(&out)
	root.SetArgs([]string{"logout"})
	err := root.Execute()
	return out.String(), deleted, err
}

func TestLogoutEndsTheSessionAndDeletesTheFile(t *testing.T) {
	signOut := &fakeSignOut{}
	out, deleted, err := runLogout(loaded(signedIn, nil), signOut)
	if err != nil || out != "Signed out.\n" || deleted != 1 || signOut.calls != 1 {
		t.Fatalf("%q %d %v", out, deleted, err)
	}
}

func TestLogoutWhenNotSignedIn(t *testing.T) {
	signOut := &fakeSignOut{}
	out, deleted, err := runLogout(loaded(console.Session{}, console.ErrSignedOut), signOut)
	if err != nil || out != "Not signed in.\n" || deleted != 1 || signOut.calls != 0 {
		t.Fatalf("%q %d %v", out, deleted, err)
	}
}

func TestLogoutKeepsTheFileWhenTheConsoleRefuses(t *testing.T) {
	_, deleted, err := runLogout(loaded(signedIn, nil), &fakeSignOut{err: console.ErrConsoleUnavailable})
	if !errors.Is(err, console.ErrConsoleUnavailable) || deleted != 0 {
		t.Fatalf("%d %v", deleted, err)
	}
}
