package console

import (
	"context"
	"fmt"
	"net/http"
)

// SignOut ends a Console session; afterwards no copy of its cookies is accepted.
type SignOut interface {
	Execute(ctx context.Context, session Session) error
}

type signOut struct {
	client *http.Client
}

func NewSignOut(client *http.Client) SignOut {
	return signOut{client: client}
}

// Execute posts the logout as a write (cookies, Origin and the CSRF header); a session the Console no longer knows
// counts as signed out.
func (s signOut) Execute(ctx context.Context, session Session) error {
	base, origin, err := OriginOf(session.ConsoleURL)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/session/logout", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Origin", origin)
	request.Header.Set(CSRFHeader, session.CSRF)
	request.AddCookie(&http.Cookie{Name: SessionCookieName, Value: session.Token})
	request.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: session.CSRF})
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConsoleUnavailable, err)
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusUnauthorized:
		return nil
	default:
		return fmt.Errorf("%w: sign-out answered %d", ErrConsoleUnavailable, response.StatusCode)
	}
}
