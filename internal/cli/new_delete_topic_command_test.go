package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestDeleteTopicCommand(t *testing.T) {
	path := "/api/clusters/kafka-a/gateway/api/v1/topics/kafka3o-cli-p112"
	accepted := map[string]string{path: `{"name":"kafka3o-cli-p112","status":"accepted"}`}

	writes := &fakeConsoleWrites{answers: accepted}
	out, prompt, err := runTopicChange(writes, "kafka3o-cli-p112\ny\n", "delete-topic", "kafka-a", "kafka3o-cli-p112")
	if err != nil || out != "Deleted topic kafka3o-cli-p112 on kafka-a.\n" || !strings.Contains(prompt, "Type the topic name to delete it: ") || len(writes.sent) != 1 || writes.sent[0].Method != "DELETE" {
		t.Fatalf("%v %q %q %+v", err, out, prompt, writes.sent)
	}

	mistyped := &fakeConsoleWrites{answers: accepted}
	if _, _, err := runTopicChange(mistyped, "kafka3o-cli-p11\ny\n", "delete-topic", "kafka-a", "kafka3o-cli-p112"); !errors.Is(err, console.ErrCanceled) || len(mistyped.sent) != 0 {
		t.Fatalf("mistyped: %v %+v", err, mistyped.sent)
	}
	yesOnly := &fakeConsoleWrites{answers: accepted}
	if _, _, err := runTopicChange(yesOnly, "", "delete-topic", "kafka-a", "kafka3o-cli-p112", "--yes"); err == nil || len(yesOnly.sent) != 0 {
		t.Fatalf("--yes alone: %v", err)
	}
	confirmed := &fakeConsoleWrites{answers: accepted}
	if _, _, err := runTopicChange(confirmed, "", "delete-topic", "kafka-a", "kafka3o-cli-p112", "--confirm", "kafka3o-cli-p112", "--yes"); err != nil || len(confirmed.sent) != 1 {
		t.Fatalf("--confirm --yes: %v", err)
	}
	production := &fakeConsoleWrites{answers: map[string]string{"/api/clusters/kafka-b/gateway/api/v1/topics/old": `{"name":"old","status":"accepted"}`}}
	if _, _, err := runTopicChange(production, "", "delete-topic", "kafka-b", "old", "--confirm", "old", "--yes"); !errors.Is(err, ErrReasonRequired) || len(production.sent) != 0 {
		t.Fatalf("production: %v", err)
	}
	if _, _, err := runTopicChange(production, "", "delete-topic", "kafka-b", "old", "--confirm", "old", "--yes", "--reason", "retired"); err != nil || production.sent[0].Reason != "retired" {
		t.Fatalf("production with reason: %v %+v", err, production.sent)
	}
}
