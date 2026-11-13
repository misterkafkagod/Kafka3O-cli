package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/backups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

const backupsPathA = "/api/clusters/kafka-a/gateway/api/v1/backups"

func backupsReads() *fakeConsoleReads {
	return &fakeConsoleReads{
		answers: map[string]string{
			"/api/clusters": whoamiReads["/api/clusters"],
			backupsPathA: `{"backups":[
				{"topic":"kafka3o-m10-verify","connector":"kafka3o-backup-kafka3o-m10-verify","location":"s3://kafka3o-backups/kafka-a/kafka3o-m10-verify/","state":"RUNNING",
				 "storage":{"objects":12,"storedBytes":2400,"lastWrite":"2026-11-13T10:00:00Z"},"settings":{"startFrom":"earliest","compression":"gzip","recordsPerFile":0},
				 "restore":{"restoreId":"r1","state":"RUNNING","restored":1,"total":4,"finished":false},"restoring":true},
				{"topic":"tert","connector":"kafka3o-backup-tert","location":"s3://kafka3o-backups/kafka-a/tert/","state":"FAILED","error":"S3 \u001b[2Jdown",
				 "storage":{"objects":0,"storedBytes":0,"lastWrite":null},"settings":{"startFrom":"latest","compression":"zstd","recordsPerFile":500}},
				{"topic":"orders","connector":"kafka3o-backup-orders","location":"s3://kafka3o-backups/kafka-a/orders/","state":"PAUSED","restoring":true}]}`,
		},
		errors: map[string]error{"/api/clusters/kafka-b/gateway/api/v1/backups": console.Problem{Status: 404, Code: "backups_not_configured", Detail: "Backups are not configured for this cluster"}},
	}
}

func runBackupsCommand(reads *fakeConsoleReads, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewBackupsCommand(SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads},
		backups.NewListBackups(clusters.NewListAccessibleClusters(reads), reads)))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"backups"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func TestBackupsCommand(t *testing.T) {
	local := time.Local
	time.Local = time.UTC
	defer func() { time.Local = local }()
	out, _, err := runBackupsCommand(backupsReads())
	want := []string{
		"CLUSTER TOPIC STATE LAST WRITE STORED SETTINGS RESTORE",
		"kafka-a kafka3o-m10-verify RUNNING 2026-11-13 10:00:00 UTC 2400 bytes in 12 objects from the oldest message, gzip, no record limit restoring 1 of 4 records",
		"kafka-a orders PAUSED unavailable unavailable unknown restoring (progress not checked)",
		"kafka-a tert FAILED nothing written yet 0 bytes in 0 objects messages from when the backup started, zstd, up to 500 records per file none",
		"", "Errors", `kafka-a tert: S3 \x1b[2Jdown`,
		"", "kafka-b: Backups are not configured for this cluster.",
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	for i := range lines {
		lines[i] = strings.Join(strings.Fields(lines[i]), " ")
	}
	if err != nil || strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestBackupsCommandJSONAndFailures(t *testing.T) {
	out, _, err := runBackupsCommand(backupsReads(), "-o", "json")
	var list struct {
		Clusters []backups.ClusterBackups  `json:"clusters"`
		Failures []clusters.ClusterFailure `json:"failures"`
	}
	if err != nil || json.Unmarshal([]byte(out), &list) != nil || len(list.Clusters) != 2 || len(list.Clusters[0].Backups) != 3 || !list.Clusters[1].NotConfigured ||
		list.Failures == nil || !strings.Contains(out, `"storedBytes": 2400`) || !strings.Contains(out, `"connector": "kafka3o-backup-tert"`) {
		t.Fatalf("%v\n%s", err, out)
	}
	failing := backupsReads()
	failing.errors[backupsPathA] = console.Problem{Status: 503, Code: "storage_unavailable", Title: "Backup storage unavailable", Detail: "The backup storage cannot be listed"}
	out, errOut, err := runBackupsCommand(failing)
	if ExitCode(err) != ExitClusterFail || errOut != "kafka-a: Status unavailable: The backup storage cannot be listed (503 storage_unavailable)\n" ||
		out != "No backups.\n\nkafka-b: Backups are not configured for this cluster.\n" {
		t.Fatalf("%v\n%q\n%q", err, out, errOut)
	}
}
