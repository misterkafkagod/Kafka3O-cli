package cli

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/backups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

const backupBase = "/api/clusters/kafka-a/gateway/api/v1/backups"

func runBackup(writes *fakeConsoleWrites, stdin string, args ...string) (string, string, error) {
	reads := &fakeConsoleReads{answers: whoamiReads}
	deps := BackupDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, Accessible: clusters.NewListAccessibleClusters(reads),
		Start: backups.NewStartBackup(writes), Run: backups.NewRunBackupAction(writes)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewBackupCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"backup"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

const startedAnswer = `{"topic":"kafka3o-cli-p119","connector":"kafka3o-backup-kafka3o-cli-p119","location":"s3://kafka3o-backups/kafka-a/kafka3o-cli-p119/","state":"RUNNING"}`

func TestBackupCommandStart(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{backupBase: startedAnswer}}
	out, prompt, err := runBackup(writes, "y\n", "start", "kafka-a", "kafka3o-cli-p119", "--start-from", "latest", "--compression", "zstd", "--records-per-file", "500")
	if err != nil || out != "Backup started for kafka3o-cli-p119. Messages are stored in s3://kafka3o-backups/kafka-a/kafka3o-cli-p119/.\n" ||
		!strings.Contains(prompt, "  settings: messages from when the backup started, zstd, up to 500 records per file\n") ||
		writes.sent[0].Body != `{"topic":"kafka3o-cli-p119","settings":{"startFrom":"latest","compression":"zstd","recordsPerFile":500}}` {
		t.Fatalf("%v\n%s\n%s\n%+v", err, out, prompt, writes.sent)
	}
	defaults := &fakeConsoleWrites{answers: map[string]string{backupBase: startedAnswer}}
	_, prompt, err = runBackup(defaults, "", "start", "kafka-a", "kafka3o-cli-p119", "--yes")
	if err != nil || defaults.sent[0].Body != `{"topic":"kafka3o-cli-p119"}` || !strings.Contains(prompt, "  settings: from the oldest message, gzip, no record limit\n") {
		t.Fatalf("defaults: %v\n%s\n%+v", err, prompt, defaults.sent)
	}
}

func TestBackupCommandActions(t *testing.T) {
	for _, test := range []struct{ kind, path, summary, done string }{
		{"pause", backupBase + "/kafka3o-cli-p119/pause", `Pause the backup of "kafka3o-cli-p119" (new messages are not stored until it resumes)`, "Pause requested for the backup of kafka3o-cli-p119.\n"},
		{"resume", backupBase + "/kafka3o-cli-p119/resume", `Resume the backup of "kafka3o-cli-p119"`, "Resume requested for the backup of kafka3o-cli-p119.\n"},
		{"delete", backupBase + "/kafka3o-cli-p119", `Delete the backup job for "kafka3o-cli-p119" (the stored objects are kept; only the job stops)`, "The backup job for kafka3o-cli-p119 was deleted. Its stored objects are kept.\n"},
	} {
		writes := &fakeConsoleWrites{answers: map[string]string{test.path: `{"topic":"kafka3o-cli-p119","action":"` + test.kind + `"}`}}
		out, prompt, err := runBackup(writes, "y\n", test.kind, "kafka-a", "kafka3o-cli-p119")
		if err != nil || out != test.done || !strings.Contains(prompt, "  "+test.summary+"\nProceed? [y/N] ") || len(writes.sent) != 1 {
			t.Fatalf("%s: %v\n%s\n%s", test.kind, err, out, prompt)
		}
	}
}

func TestBackupCommandRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{}
	for args, want := range map[string]string{
		"start kafka-a t --compression lz4 --yes":          `--compression must be gzip, zstd, snappy or none, not "lz4"`,
		"start kafka-a t --start-from now --yes":           `--start-from must be earliest or latest, not "now"`,
		"start kafka-a t --records-per-file 1000001 --yes": "--records-per-file must be between 0 (no limit) and 1000000, not 1000001",
		"pause kafka-a bad/topic --yes":                    `invalid topic name "bad/topic"`,
	} {
		if _, _, err := runBackup(writes, "", strings.Fields(args)...); err == nil || !strings.Contains(err.Error(), want) || len(writes.sent) != 0 {
			t.Fatalf("%s: %v", args, err)
		}
	}
	if _, _, err := runBackup(writes, "n\n", "delete", "kafka-a", "t"); !errors.Is(err, console.ErrCanceled) || len(writes.sent) != 0 {
		t.Fatalf("cancel: %v", err)
	}
	if _, _, err := runBackup(writes, "", "start", "kafka-b", "t", "--yes"); !errors.Is(err, ErrReasonRequired) || len(writes.sent) != 0 {
		t.Fatalf("production: %v", err)
	}
	exists := &fakeConsoleWrites{errors: map[string]error{backupBase: console.Problem{Status: 409, Code: "backup_exists", Detail: "This topic already has a backup"}}}
	if _, _, err := runBackup(exists, "", "start", "kafka-a", "t", "--yes"); err == nil || err.Error() != "refused: This topic is already backed up." {
		t.Fatalf("exists: %v", err)
	}
	missing := &fakeConsoleWrites{errors: map[string]error{backupBase + "/t/pause": console.Problem{Status: 404, Code: "not_found", Detail: "This topic has no backup"}}}
	if _, _, err := runBackup(missing, "", "pause", "kafka-a", "t", "--yes"); err == nil || err.Error() != "topic t has no backup on kafka-a" {
		t.Fatalf("missing: %v", err)
	}
	unknown := &fakeConsoleWrites{errors: map[string]error{backupBase + "/t": fmt.Errorf("%w (503 connect_action_unknown)", console.ErrOutcomeUnknown)}}
	if _, _, err := runBackup(unknown, "", "delete", "kafka-a", "t", "--yes"); ExitCode(err) != ExitOutcomeUnknown {
		t.Fatalf("unknown: %v", err)
	}
	withReason := &fakeConsoleWrites{answers: map[string]string{"/api/clusters/kafka-b/gateway/api/v1/backups/t/resume": `{"topic":"t","action":"resume"}`}}
	if _, _, err := runBackup(withReason, "", "resume", "kafka-b", "t", "--yes", "--reason", "INC-9"); err != nil || withReason.sent[0].Reason != "INC-9" {
		t.Fatalf("reason: %v", err)
	}
}
