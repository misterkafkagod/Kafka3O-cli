package console

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

// GetCSV reads a CSV download from the Console, unchanged.
type GetCSV interface {
	Execute(ctx context.Context, session Session, path string) ([]byte, error)
}

type getCSV struct {
	client *http.Client
}

func NewGetCSV(client *http.Client) GetCSV {
	return getCSV{client: client}
}

// Execute sends the session cookies like GetJSON; 401 is ErrSignedOut, other failures are the Console's problem,
// and a success that is not text/csv is refused.
func (g getCSV) Execute(ctx context.Context, session Session, path string) ([]byte, error) {
	base, _, err := OriginOf(session.ConsoleURL)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/"+strings.TrimLeft(path, "/"), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "text/csv, application/problem+json")
	request.AddCookie(&http.Cookie{Name: SessionCookieName, Value: session.Token})
	request.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: session.CSRF})
	response, err := g.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConsoleUnavailable, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: read the answer: %v", ErrConsoleUnavailable, err)
	}
	if response.StatusCode == http.StatusUnauthorized {
		return nil, ErrSignedOut
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		var problem Problem
		_ = json.Unmarshal(body, &problem)
		problem.Status = response.StatusCode
		return nil, problem
	}
	if mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type")); err != nil || mediaType != "text/csv" {
		return nil, fmt.Errorf("%w: the answer is not CSV", ErrConsoleUnavailable)
	}
	return body, nil
}
