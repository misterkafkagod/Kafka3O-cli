package console

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// GetJSON reads a Console path (for example /api/clusters, or /api/clusters/{id}/gateway/api/v1/topics) as the
// signed-in user.
type GetJSON interface {
	Execute(ctx context.Context, session Session, path string, into any) error
}

type getJSON struct {
	client *http.Client
}

func NewGetJSON(client *http.Client) GetJSON {
	return getJSON{client: client}
}

// Execute sends the session cookies; 401 means the session is over (ErrSignedOut), any other failure carries the
// status and the problem's code and detail so callers can show why.
func (g getJSON) Execute(ctx context.Context, session Session, path string, into any) error {
	base, _, err := OriginOf(session.ConsoleURL)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/"+strings.TrimLeft(path, "/"), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	request.AddCookie(&http.Cookie{Name: SessionCookieName, Value: session.Token})
	request.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: session.CSRF})
	response, err := g.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConsoleUnavailable, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("%w: read the answer: %v", ErrConsoleUnavailable, err)
	}
	if response.StatusCode == http.StatusUnauthorized {
		return ErrSignedOut
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		var problem Problem
		_ = json.Unmarshal(body, &problem)
		problem.Status = response.StatusCode
		return problem
	}
	if err := json.Unmarshal(body, into); err != nil {
		return fmt.Errorf("%w: the answer is not the expected JSON: %v", ErrConsoleUnavailable, err)
	}
	return nil
}
