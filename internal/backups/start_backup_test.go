package backups

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestStartBackup(t *testing.T) {
	answer := `{"topic":"kafka3o-cli-p119","connector":"kafka3o-backup-kafka3o-cli-p119","location":"s3://kafka3o-backups/kafka-a/kafka3o-cli-p119/","state":"RUNNING"}`
	send := &fakeSendJSON{answer: answer}
	created, err := NewStartBackup(send).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-cli-p119", nil, "")
	if err != nil || send.method != "POST" || send.path != "/api/clusters/kafka-a/gateway/api/v1/backups" || send.body != `{"topic":"kafka3o-cli-p119"}` ||
		created.Location != "s3://kafka3o-backups/kafka-a/kafka3o-cli-p119/" || created.Cluster != "kafka-a" {
		t.Fatalf("%+v %v %+v", created, err, send)
	}
	send = &fakeSendJSON{answer: answer}
	if _, err := NewStartBackup(send).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-cli-p119", &Settings{StartFrom: "latest", Compression: "zstd"}, "INC-3"); err != nil ||
		send.body != `{"topic":"kafka3o-cli-p119","settings":{"startFrom":"latest","compression":"zstd"}}` || send.reason != "INC-3" {
		t.Fatalf("%v %+v", err, send)
	}
	if _, err := NewStartBackup(&fakeSendJSON{answer: `{"topic":"other"}`}).Execute(context.Background(), console.Session{}, "kafka-a", "kafka3o-cli-p119", nil, ""); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Fatalf("echo: %v", err)
	}
	exists := console.Problem{Status: 409, Code: "backup_exists"}
	if _, err := NewStartBackup(&fakeSendJSON{err: exists}).Execute(context.Background(), console.Session{}, "kafka-a", "t", nil, ""); !errors.Is(err, exists) {
		t.Fatalf("exists: %v", err)
	}
}
