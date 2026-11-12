package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeSignIn struct {
	err                     error
	console, user, password string
}

func (f *fakeSignIn) Execute(_ context.Context, consoleURL, username, password string) (console.Session, error) {
	f.console, f.user, f.password = consoleURL, username, password
	if f.err != nil {
		return console.Session{}, f.err
	}
	return console.Session{ConsoleURL: "http://localhost:5173", Username: username, ExpiresAt: time.Date(2026, 11, 12, 20, 6, 0, 0, time.UTC), Token: "secret-token", CSRF: "secret-csrf"}, nil
}

type fakePassword struct {
	password string
	err      error
	asked    int
}

func (f *fakePassword) Execute(string) (string, error) {
	f.asked++
	return f.password, f.err
}

type savedSessions struct {
	path    string
	session *console.Session
}

func (s *savedSessions) save(path string, session console.Session) error {
	s.path, s.session = path, &session
	return nil
}

func runLogin(t *testing.T, signIn *fakeSignIn, password *fakePassword, stdin string, args ...string) (string, string, *savedSessions, error) {
	t.Helper()
	saved := &savedSessions{}
	root := NewRootCommand(NewLoginCommand(LoginDependencies{SignIn: signIn, SaveSession: saved.save, SessionPath: "/tmp/kafka3o/session.json", ReadPassword: password}))
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"login"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), saved, err
}

func TestLoginSavesTheSessionAndSaysWhoSignedIn(t *testing.T) {
	signIn := &fakeSignIn{}
	out, errOut, saved, err := runLogin(t, signIn, &fakePassword{password: "s3cret"}, "admin\n", "--console", "http://localhost:5173")
	if err != nil {
		t.Fatal(err)
	}
	if signIn.user != "admin" || signIn.password != "s3cret" || signIn.console != "http://localhost:5173" {
		t.Fatalf("sign-in got %+v", signIn)
	}
	if saved.session == nil || saved.path != "/tmp/kafka3o/session.json" || saved.session.Token != "secret-token" {
		t.Fatalf("saved %+v", saved)
	}
	if !strings.HasPrefix(out, "Signed in to http://localhost:5173 as admin until ") || !strings.Contains(errOut, "Username: ") {
		t.Fatalf("out %q err %q", out, errOut)
	}
	if strings.Contains(out+errOut, "s3cret") || strings.Contains(out+errOut, "secret-token") || strings.Contains(out+errOut, "secret-csrf") {
		t.Fatal("a secret was printed")
	}
}

func TestLoginWithPasswordStdin(t *testing.T) {
	signIn := &fakeSignIn{}
	password := &fakePassword{}
	_, _, saved, err := runLogin(t, signIn, password, "s3cret\n", "--console", "http://localhost:5173", "--username", "admin", "--password-stdin")
	if err != nil || signIn.password != "s3cret" || password.asked != 0 || saved.session == nil {
		t.Fatalf("err %v, password %q, asked %d", err, signIn.password, password.asked)
	}
}

func TestLoginFailuresStoreNothing(t *testing.T) {
	for name, want := range map[error]int{
		console.ErrSignInFailed:           ExitNotSignedIn,
		console.ErrPasswordChangeRequired: ExitNotSignedIn,
		console.ErrSignInThrottled:        ExitNotSignedIn,
		console.ErrConsoleUnavailable:     ExitUsage,
	} {
		_, _, saved, err := runLogin(t, &fakeSignIn{err: name}, &fakePassword{password: "wrong"}, "admin\n", "--console", "http://localhost:5173")
		if !errors.Is(err, name) || ExitCode(err) != want || saved.session != nil || !strings.HasPrefix(err.Error(), "sign-in failed: ") {
			t.Errorf("%v: err %v, exit %d, saved %v", name, err, ExitCode(err), saved.session)
		}
	}
}

func TestLoginUsageErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"no console":               {},
		"stdin without username":   {"--console", "http://localhost:5173", "--password-stdin"},
		"stdin without a password": {"--console", "http://localhost:5173", "--username", "admin", "--password-stdin"},
	} {
		signIn := &fakeSignIn{}
		_, _, saved, err := runLogin(t, signIn, &fakePassword{password: "x"}, "", args...)
		if err == nil || ExitCode(err) != ExitUsage || signIn.user != "" || saved.session != nil {
			t.Errorf("%s: err %v", name, err)
		}
	}
	password := &fakePassword{err: errors.New("standard input is not a terminal; use --password-stdin")}
	if _, _, _, err := runLogin(t, &fakeSignIn{}, password, "admin\n", "--console", "http://localhost:5173"); err == nil || !strings.Contains(err.Error(), "--password-stdin") {
		t.Errorf("no terminal: %v", err)
	}
}
