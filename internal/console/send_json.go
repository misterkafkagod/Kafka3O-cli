package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
)

// SendJSON sends a change (POST or DELETE) to a Console path as the signed-in user.
type SendJSON interface {
	Execute(ctx context.Context, session Session, method, path string, body any, reason string, into any) error
}

type sendJSON struct {
	client *http.Client
}

func NewSendJSON(client *http.Client) SendJSON {
	return sendJSON{client: client}
}

// Execute sends the cookies, Origin and the CSRF header like the browser; a non-empty reason travels as
// X-Kafka3O-Change-Reason, which the Console requires for Production changes and binds into the audited request.
// 401 means the session is over; the Console's "may have been applied" answers become ErrOutcomeUnknown.
func (s sendJSON) Execute(ctx context.Context, session Session, method, path string, body any, reason string, into any) error {
	base, origin, err := OriginOf(session.ConsoleURL)
	if err != nil {
		return err
	}
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, base+"/"+strings.TrimLeft(path, "/"), payload)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Origin", origin)
	request.Header.Set(CSRFHeader, session.CSRF)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if reason != "" {
		request.Header.Set("X-Kafka3O-Change-Reason", reason)
	}
	request.AddCookie(&http.Cookie{Name: SessionCookieName, Value: session.Token})
	request.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: session.CSRF})
	response, err := s.client.Do(request)
	if err != nil {
		// A failed dial sent nothing; any later failure may have reached the Console.
		var dial *net.OpError
		if errors.As(err, &dial) && dial.Op == "dial" {
			return fmt.Errorf("%w: %v", ErrConsoleUnavailable, err)
		}
		return fmt.Errorf("%w: %v", ErrOutcomeUnknown, err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("%w: read the answer: %v", ErrOutcomeUnknown, err)
	}
	if response.StatusCode == http.StatusUnauthorized {
		return ErrSignedOut
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		var problem Problem
		_ = json.Unmarshal(answer, &problem)
		problem.Status = response.StatusCode
		switch problem.Code {
		case "gateway_outcome_unknown", "outcome_unknown", "connect_action_unknown":
			return fmt.Errorf("%w (%s)", ErrOutcomeUnknown, problem.Error())
		}
		return problem
	}
	if into == nil || len(bytes.TrimSpace(answer)) == 0 {
		return nil
	}
	if err := json.Unmarshal(answer, into); err != nil {
		return fmt.Errorf("%w: the answer is not the expected JSON: %v", ErrOutcomeUnknown, err)
	}
	return nil
}
