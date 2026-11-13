package accounts

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestCreateAccount(t *testing.T) {
	send := &fakeSendJSON{answer: `{"operationId":"o","account":{"id":"a2","username":"kafka3o-cli-test","enabled":true,"mustChangePassword":true}}`}
	account, err := NewCreateAccount(send).Execute(context.Background(), console.Session{}, "Kafka3O-CLI-Test", "temporary-password")
	if err != nil || account.ID != "a2" || send.method != "POST" || send.path != "/api/admin/local-accounts" ||
		send.body != `{"username":"Kafka3O-CLI-Test","temporaryPassword":"temporary-password"}` {
		t.Fatalf("%+v %v %+v", account, err, send)
	}
	if _, err := NewCreateAccount(&fakeSendJSON{answer: `{"account":{"id":"a3","username":"other"}}`}).Execute(context.Background(), console.Session{}, "x", "temporary-password"); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("echo: %v", err)
	}
}

func TestResetPassword(t *testing.T) {
	send := &fakeSendJSON{answer: `{"operationId":"o","signInRequired":true}`}
	result, err := NewResetPassword(send).Execute(context.Background(), console.Session{}, Account{ID: "a/2"}, "new-temporary-1")
	if err != nil || !result.SignInRequired || send.method != "POST" || send.path != "/api/admin/local-accounts/a%2F2/password-reset" || send.body != `{"temporaryPassword":"new-temporary-1"}` {
		t.Fatalf("%+v %v %+v", result, err, send)
	}
}

func TestSetAccountEnabled(t *testing.T) {
	send := &fakeSendJSON{answer: `{"operationId":"o","enabled":false,"signInRequired":true}`}
	result, err := NewSetAccountEnabled(send).Execute(context.Background(), console.Session{}, Account{ID: "a2", Username: "t"}, false)
	if err != nil || !result.SignInRequired || send.method != "PUT" || send.path != "/api/admin/local-accounts/a2/enabled" || send.body != `{"enabled":false}` {
		t.Fatalf("%+v %v %+v", result, err, send)
	}
	for _, answer := range []string{`{"enabled":true}`, `{}`} {
		if _, err := NewSetAccountEnabled(&fakeSendJSON{answer: answer}).Execute(context.Background(), console.Session{}, Account{ID: "a2"}, false); !errors.Is(err, console.ErrOutcomeUnknown) {
			t.Fatalf("%s: %v", answer, err)
		}
	}
}
