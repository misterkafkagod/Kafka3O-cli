package connect

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestRunConnectorAction(t *testing.T) {
	base := "/api/clusters/kafka-a/gateway/api/v1/connect/connectors/"
	for _, test := range []struct {
		action ConnectorAction
		path   string
		answer string
	}{
		{ConnectorAction{Kind: ActionPause, Connector: "kafka3o-verify-paused"}, base + "kafka3o-verify-paused/pause", `{"connector":"kafka3o-verify-paused","action":"pause"}`},
		{ConnectorAction{Kind: ActionResume, Connector: "a/b c"}, base + "a%2Fb%20c/resume", `{"connector":"a/b c","action":"resume"}`},
		{ConnectorAction{Kind: ActionRestart, Connector: "x"}, base + "x/restart", `{"connector":"x","action":"restart"}`},
		{ConnectorAction{Kind: ActionRestartTask, Connector: "x", Task: 2}, base + "x/tasks/2/restart", `{"connector":"x","action":"restart-task","task":2}`},
	} {
		send := &fakeSendJSON{answer: test.answer}
		if err := NewRunConnectorAction(send).Execute(context.Background(), console.Session{}, "kafka-a", test.action, "INC-1"); err != nil ||
			send.method != "POST" || send.path != test.path || send.body != "null" || send.reason != "INC-1" {
			t.Fatalf("%+v: %v %+v", test.action, err, send)
		}
	}
}

func TestRunConnectorActionFailures(t *testing.T) {
	task := ConnectorAction{Kind: ActionRestartTask, Connector: "x", Task: 9}
	missing := &fakeSendJSON{err: console.Problem{Status: 404, Code: "not_found", Detail: "the connector or task does not exist"}}
	err := NewRunConnectorAction(missing).Execute(context.Background(), console.Session{}, "kafka-a", task, "")
	if !errors.As(err, new(ErrConnectorNotFound)) || err.Error() != "task 9 of connector x was not found on kafka-a" {
		t.Fatalf("not found: %v", err)
	}
	if err := NewRunConnectorAction(missing).Execute(context.Background(), console.Session{}, "kafka-a", ConnectorAction{Kind: ActionPause, Connector: "x"}, ""); err.Error() != "connector x was not found on kafka-a" {
		t.Fatalf("not found: %v", err)
	}
	for _, answer := range []string{`{"connector":"x","action":"restart-task","task":1}`, `{"connector":"x","action":"restart-task"}`, `{"connector":"y","action":"restart-task","task":9}`} {
		if err := NewRunConnectorAction(&fakeSendJSON{answer: answer}).Execute(context.Background(), console.Session{}, "kafka-a", task, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
			t.Fatalf("%s: %v", answer, err)
		}
	}
	if err := NewRunConnectorAction(&fakeSendJSON{answer: `{"connector":"x","action":"pause","task":0}`}).Execute(context.Background(), console.Session{}, "kafka-a", ConnectorAction{Kind: ActionPause, Connector: "x"}, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("unexpected task: %v", err)
	}
	rejected := console.Problem{Status: 409, Code: "connect_rejected", Detail: "Kafka Connect rejected the action; refresh and try again"}
	if err := NewRunConnectorAction(&fakeSendJSON{err: rejected}).Execute(context.Background(), console.Session{}, "kafka-a", task, ""); !errors.Is(err, rejected) {
		t.Fatalf("rejected: %v", err)
	}
}
