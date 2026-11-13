package console

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type recordedWrite struct {
	method, path, body, reason, contentType string
}

func newWriteConsole(t *testing.T, answer func(w http.ResponseWriter, r *http.Request)) (*[]recordedWrite, string) {
	t.Helper()
	writes := &[]recordedWrite{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, _ := r.Cookie(SessionCookieName)
		if token == nil || token.Value != "token-1" {
			problem(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if r.Header.Get("Origin") != "http://"+r.Host || r.Header.Get(CSRFHeader) != "csrf-1" {
			problem(w, http.StatusForbidden, "csrf_rejected")
			return
		}
		body, _ := io.ReadAll(r.Body)
		*writes = append(*writes, recordedWrite{r.Method, r.URL.RequestURI(), string(body), r.Header.Get("X-Kafka3O-Change-Reason"), r.Header.Get("Content-Type")})
		answer(w, r)
	}))
	t.Cleanup(server.Close)
	return writes, server.URL
}

func TestSendJSONSendsTheWriteHeadersAndBody(t *testing.T) {
	writes, url := newWriteConsole(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"partition":0,"offset":"66"}`))
	})
	session := Session{ConsoleURL: url, Token: "token-1", CSRF: "csrf-1"}
	var result struct {
		Partition int32  `json:"partition"`
		Offset    string `json:"offset"`
	}
	err := NewSendJSON(http.DefaultClient).Execute(context.Background(), session, http.MethodPost, "/api/clusters/kafka-b/gateway/api/v1/topics/t/messages",
		map[string]string{"value": "hello"}, "INC-42 replay", &result)
	if err != nil || result.Offset != "66" || len(*writes) != 1 {
		t.Fatalf("%+v %v %+v", result, err, *writes)
	}
	got := (*writes)[0]
	if got.method != "POST" || got.body != `{"value":"hello"}` || got.reason != "INC-42 replay" || got.contentType != "application/json" {
		t.Fatalf("%+v", got)
	}
	if err := NewSendJSON(http.DefaultClient).Execute(context.Background(), session, http.MethodDelete, "/api/x", nil, "", nil); err != nil {
		t.Fatal(err)
	}
	if got := (*writes)[1]; got.method != "DELETE" || got.body != "" || got.reason != "" || got.contentType != "" {
		t.Fatalf("delete %+v", got)
	}
}

func TestSendJSONFailures(t *testing.T) {
	_, url := newWriteConsole(t, func(w http.ResponseWriter, r *http.Request) {
		switch code := strings.TrimPrefix(r.URL.Path, "/"); {
		case strings.HasSuffix(code, "_unknown"):
			problem(w, http.StatusServiceUnavailable, code)
		default:
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "forbidden", "detail": "Not permitted: change_reason_required"})
		}
	})
	send := NewSendJSON(http.DefaultClient)
	session := Session{ConsoleURL: url, Token: "token-1", CSRF: "csrf-1"}
	for _, path := range []string{"/gateway_outcome_unknown", "/outcome_unknown", "/connect_action_unknown", "/topic_write_unknown", "/delivery_unknown",
		"/configuration_write_unknown", "/partition_write_unknown", "/offset_reset_unknown"} {
		if err := send.Execute(context.Background(), session, "POST", path, nil, "", nil); !errors.Is(err, ErrOutcomeUnknown) {
			t.Errorf("%s: %v", path, err)
		}
	}
	var problemErr Problem
	if err := send.Execute(context.Background(), session, "POST", "/denied", nil, "", nil); !errors.As(err, &problemErr) || problemErr.Status != 403 || problemErr.Detail != "Not permitted: change_reason_required" {
		t.Errorf("denied: %v", err)
	}
	if err := send.Execute(context.Background(), Session{ConsoleURL: url, Token: "old", CSRF: "csrf-1"}, "POST", "/x", nil, "", nil); !errors.Is(err, ErrSignedOut) {
		t.Errorf("signed out: %v", err)
	}
	if err := send.Execute(context.Background(), Session{ConsoleURL: "http://127.0.0.1:1", Token: "t", CSRF: "c"}, "POST", "/x", nil, "", nil); !errors.Is(err, ErrConsoleUnavailable) || errors.Is(err, ErrOutcomeUnknown) {
		t.Errorf("refused connection sent nothing: %v", err)
	}
}
