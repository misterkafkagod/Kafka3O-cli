package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SignIn signs in to the Console with a local account.
type SignIn interface {
	Execute(ctx context.Context, consoleURL, username, password string) (Session, error)
}

type signIn struct {
	client  *http.Client
	signOut SignOut
}

// NewSignIn uses the client for the request; a session whose password must be changed is ended with signOut.
func NewSignIn(client *http.Client, signOut SignOut) SignIn {
	return signIn{client: client, signOut: signOut}
}

// Execute follows the cli-console-session recipe: no cookies are sent, both cookies and the expiry are kept, and a
// session that must change its password is signed out again so nothing usable is left behind.
func (s signIn) Execute(ctx context.Context, consoleURL, username, password string) (Session, error) {
	base, origin, err := OriginOf(consoleURL)
	if err != nil {
		return Session{}, err
	}
	body, err := json.Marshal(map[string]string{"username": username, "password": password})
	if err != nil {
		return Session{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/session/local", bytes.NewReader(body))
	if err != nil {
		return Session{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Origin", origin)
	response, err := s.client.Do(request)
	if err != nil {
		return Session{}, fmt.Errorf("%w: %v", ErrConsoleUnavailable, err)
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	var problem struct {
		Code string `json:"code"`
	}
	switch {
	case response.StatusCode == http.StatusUnauthorized:
		return Session{}, ErrSignInFailed
	case response.StatusCode == http.StatusTooManyRequests:
		return Session{}, ErrSignInThrottled
	case response.StatusCode != http.StatusOK:
		_ = json.Unmarshal(payload, &problem)
		return Session{}, fmt.Errorf("%w: sign-in answered %d %s", ErrConsoleUnavailable, response.StatusCode, problem.Code)
	}
	var signedIn struct {
		AccountID          string    `json:"accountId"`
		Username           string    `json:"username"`
		MustChangePassword bool      `json:"mustChangePassword"`
		AbsoluteExpiresAt  time.Time `json:"absoluteExpiresAt"`
	}
	if err := json.Unmarshal(payload, &signedIn); err != nil {
		return Session{}, fmt.Errorf("%w: invalid sign-in answer", ErrConsoleUnavailable)
	}
	session := Session{ConsoleURL: base, Username: signedIn.Username, AccountID: signedIn.AccountID, ExpiresAt: signedIn.AbsoluteExpiresAt}
	for _, cookie := range response.Cookies() {
		switch cookie.Name {
		case SessionCookieName:
			session.Token = cookie.Value
		case CSRFCookieName:
			session.CSRF = cookie.Value
		}
	}
	if session.Token == "" || session.CSRF == "" {
		return Session{}, fmt.Errorf("%w: the sign-in answer had no session cookies", ErrConsoleUnavailable)
	}
	if signedIn.MustChangePassword {
		_ = s.signOut.Execute(ctx, session)
		return Session{}, ErrPasswordChangeRequired
	}
	return session, nil
}
