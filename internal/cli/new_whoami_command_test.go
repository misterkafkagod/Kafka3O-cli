package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

var whoamiReads = map[string]string{
	"/api/session":  `{"accountId":"acc-1","username":"admin","authSource":"local","mustChangePassword":false,"absoluteExpiresAt":"2026-11-12T20:12:02Z","administration":{"access":true,"audit":true}}`,
	"/api/clusters": `{"clusters":[{"id":"kafka-a","name":"Kafka A","environment":"NON_PRODUCTION","effectiveRole":"ADMIN"},{"id":"kafka-b","name":"Kafka B","environment":"PRODUCTION","effectiveRole":"ADMIN"}]}`,
}

func runWhoami(reads *fakeConsoleReads, load func() (console.Session, error), args ...string) (string, error) {
	var out bytes.Buffer
	root := NewRootCommand(NewWhoamiCommand(SessionDependencies{LoadSession: load, GetJSON: reads}, clusters.NewListAccessibleClusters(reads)))
	root.SetOut(&out)
	root.SetArgs(append([]string{"whoami"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestWhoamiTable(t *testing.T) {
	out, err := runWhoami(&fakeConsoleReads{answers: whoamiReads}, loaded(signedIn, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Account:         admin (local)", "Console:         http://localhost:5173", "Administration:  access, audit",
		"CLUSTER  NAME     ENVIRONMENT     ROLE", "kafka-a  Kafka A  Non-production  ADMIN", "kafka-b  Kafka B  Production      ADMIN"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, signedIn.Token) || strings.Contains(out, signedIn.CSRF) {
		t.Fatal("the session token was printed")
	}
}

func TestWhoamiJSON(t *testing.T) {
	out, err := runWhoami(&fakeConsoleReads{answers: whoamiReads}, loaded(signedIn, nil), "--output", "json")
	var got whoami
	if err != nil || json.Unmarshal([]byte(out), &got) != nil || got.Username != "admin" || len(got.Clusters) != 2 || got.Clusters[1].Environment != "PRODUCTION" || !got.Administration["audit"] {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestWhoamiSignedOut(t *testing.T) {
	if _, err := runWhoami(&fakeConsoleReads{}, loaded(console.Session{}, console.ErrSignedOut)); ExitCode(err) != ExitNotSignedIn {
		t.Fatalf("not signed in: %v", err)
	}
	ended := &fakeConsoleReads{errors: map[string]error{"/api/session": console.ErrSignedOut}}
	if _, err := runWhoami(ended, loaded(signedIn, nil)); !errors.Is(err, console.ErrSignedOut) || ExitCode(err) != ExitNotSignedIn {
		t.Fatalf("ended: %v", err)
	}
}
