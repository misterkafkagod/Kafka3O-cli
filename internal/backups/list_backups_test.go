package backups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

type fakeGetJSON struct {
	answers map[string]string
	errors  map[string]error
}

func (f *fakeGetJSON) Execute(_ context.Context, _ console.Session, path string, into any) error {
	if err := f.errors[path]; err != nil {
		return err
	}
	answer, ok := f.answers[path]
	if !ok {
		return fmt.Errorf("unexpected path %s", path)
	}
	return json.Unmarshal([]byte(answer), into)
}

const backupsPath = "/api/clusters/kafka-a/gateway/api/v1/backups"

func backupReads() map[string]string {
	return map[string]string{
		"/api/clusters": `{"clusters":[{"id":"kafka-a"},{"id":"kafka-b"}]}`,
		backupsPath: `{"backups":[
			{"topic":"zeta","connector":"kafka3o-backup-zeta","location":"s3://b/kafka3o/zeta/","state":"RUNNING","storage":{"objects":2,"storedBytes":900,"lastWrite":"2026-11-13T10:00:00Z"},
			 "settings":{"startFrom":"earliest","compression":"zstd","recordsPerFile":0},"restore":{"restoreId":"r1","state":"RUNNING","restored":4,"total":9,"finished":false},"restoring":true},
			{"topic":"alpha","connector":"kafka3o-backup-alpha","location":"s3://b/kafka3o/alpha/","state":"FAILED","error":"S3 down"}]}`,
	}
}

func TestListBackups(t *testing.T) {
	get := &fakeGetJSON{answers: backupReads(), errors: map[string]error{
		"/api/clusters/kafka-b/gateway/api/v1/backups": console.Problem{Status: 404, Code: "backups_not_configured", Detail: "Backups are not configured for this cluster"}}}
	found, failures, err := NewListBackups(clusters.NewListAccessibleClusters(get), get).Execute(context.Background(), console.Session{}, "")
	if err != nil || len(failures) != 0 || len(found) != 2 {
		t.Fatalf("%+v %+v %v", found, failures, err)
	}
	a := found[0]
	if a.Cluster != "kafka-a" || len(a.Backups) != 2 || a.Backups[0].Topic != "alpha" || a.Backups[0].Error != "S3 down" || a.Backups[0].Cluster != "kafka-a" ||
		a.Backups[1].Storage.StoredBytes != 900 || a.Backups[1].Settings.Compression != "zstd" || a.Backups[1].Restore.Restored != 4 || !a.Backups[1].Restoring {
		t.Fatalf("kafka-a %+v", a)
	}
	if !found[1].NotConfigured || found[1].Backups == nil {
		t.Fatalf("kafka-b %+v", found[1])
	}
}

func TestListBackupsFailures(t *testing.T) {
	failing := &fakeGetJSON{answers: backupReads(), errors: map[string]error{backupsPath: console.Problem{Status: 503, Code: "connect_unreachable", Detail: "Kafka Connect is unreachable"}}}
	found, failures, err := NewListBackups(clusters.NewListAccessibleClusters(failing), failing).Execute(context.Background(), console.Session{}, "kafka-a")
	if err != nil || len(found) != 0 || len(failures) != 1 || failures[0].Cluster != "kafka-a" {
		t.Fatalf("%+v %+v %v", found, failures, err)
	}
	signedOut := &fakeGetJSON{answers: backupReads(), errors: map[string]error{backupsPath: console.ErrSignedOut}}
	if _, _, err := NewListBackups(clusters.NewListAccessibleClusters(signedOut), signedOut).Execute(context.Background(), console.Session{}, "kafka-a"); !errors.Is(err, console.ErrSignedOut) {
		t.Fatalf("signed out: %v", err)
	}
}
