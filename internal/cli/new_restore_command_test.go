package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/backups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

const restorePath = "/api/clusters/kafka-a/gateway/api/v1/backups/kafka3o-m10-verify/restore"

type noSleep struct{}

func (noSleep) Execute(context.Context, time.Duration) error { return nil }

// sequencedReads answers the backups listing with the next prepared answer each time.
type sequencedReads struct {
	*fakeConsoleReads
	listings []string
	calls    int
}

func (s *sequencedReads) Execute(ctx context.Context, session console.Session, path string, into any) error {
	if path == backupsPathA {
		s.answers[path] = s.listings[min(s.calls, len(s.listings)-1)]
		s.calls++
	}
	return s.fakeConsoleReads.Execute(ctx, session, path, into)
}

func runRestore(writes *fakeConsoleWrites, listings []string, stdin string, args ...string) (string, string, error) {
	reads := &sequencedReads{fakeConsoleReads: &fakeConsoleReads{answers: map[string]string{"/api/clusters": whoamiReads["/api/clusters"]}}, listings: listings}
	accessible := clusters.NewListAccessibleClusters(reads)
	deps := BackupDependencies{Session: SessionDependencies{LoadSession: loaded(signedIn, nil), GetJSON: reads}, Accessible: accessible,
		Restore: backups.NewStartRestore(writes), Wait: backups.NewWaitForRestore(backups.NewListBackups(accessible, reads), noSleep{}, time.Second)}
	var out, errOut bytes.Buffer
	root := NewRootCommand(NewRestoreCommand(deps))
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(append([]string{"restore"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func restoreListing(restore string) string {
	return `{"backups":[{"topic":"kafka3o-m10-verify","connector":"kafka3o-backup-kafka3o-m10-verify","state":"PAUSED","restoring":true,"restore":` + restore + `}]}`
}

func TestRestoreCommand(t *testing.T) {
	writes := &fakeConsoleWrites{answers: map[string]string{restorePath: `{"topic":"kafka3o-m10-verify","restoreId":"r1","objects":13,"skipped":0}`}}
	out, prompt, err := runRestore(writes, nil, "y\n", "kafka-a", "kafka3o-m10-verify")
	if err != nil || out != "Restore started for kafka3o-m10-verify from 13 stored object(s).\nFollow it with: kafka3o backups --cluster kafka-a (listing backups also finishes the restore)\n" ||
		!strings.Contains(prompt, "  Restore the backup of \"kafka3o-m10-verify\" into \"kafka3o-m10-verify\"\n  the saved messages are appended to the topic, so messages still in it may be duplicated\n  the topic's backup pauses until the restore finishes\nProceed? [y/N] ") ||
		writes.sent[0].Body != "" {
		t.Fatalf("%v\n%s\n%s\n%+v", err, out, prompt, writes.sent)
	}
}

func TestRestoreCommandSelectionAndWait(t *testing.T) {
	local := time.Local
	time.Local = time.UTC
	defer func() { time.Local = local }()
	writes := &fakeConsoleWrites{answers: map[string]string{restorePath: `{"topic":"kafka3o-m10-verify","restoreId":"r1","objects":4,"skipped":2}`}}
	listings := []string{
		restoreListing(`{"restoreId":"r1","state":"RUNNING","restored":1,"total":3,"finished":false}`),
		restoreListing(`{"restoreId":"r1","state":"RUNNING","restored":3,"total":3,"finished":true}`),
	}
	out, prompt, err := runRestore(writes, listings, "", "kafka-a", "kafka3o-m10-verify", "--partitions", "1", "--from", "2026-11-13T10:00:00Z", "--yes", "--wait")
	if err != nil || out != "Restore started for kafka3o-m10-verify from 4 stored object(s).\n2 older object(s) written with another compression will not be restored.\nrestoring 1 of 3 records\nRestore finished: 3 records restored.\n" ||
		!strings.Contains(prompt, `Restore the backed-up messages of "kafka3o-m10-verify" in partition 1 from 2026-11-13 10:00:00 UTC into "kafka3o-m10-verify"`) ||
		writes.sent[0].Body != `{"from":"2026-11-13T10:00:00Z","partitions":[1]}` {
		t.Fatalf("%v\n%s\n%s\n%+v", err, out, prompt, writes.sent)
	}
	failed := &fakeConsoleWrites{answers: writes.answers}
	_, _, err = runRestore(failed, []string{restoreListing(`{"restoreId":"r1","state":"FAILED","restored":1,"total":3,"finished":false,"error":"S3 down"}`)}, "", "kafka-a", "kafka3o-m10-verify", "--yes", "--wait")
	if err == nil || err.Error() != "restore failed after 1 of 3 records: S3 down: at least one cluster could not answer" || ExitCode(err) != ExitClusterFail {
		t.Fatalf("failed: %v", err)
	}
	gone := &fakeConsoleWrites{answers: writes.answers}
	if out, _, err := runRestore(gone, []string{`{"backups":[{"topic":"kafka3o-m10-verify","state":"RUNNING"}]}`}, "", "kafka-a", "kafka3o-m10-verify", "--yes", "--wait"); err != nil || !strings.HasSuffix(out, "Restore finished.\n") {
		t.Fatalf("gone: %v\n%s", err, out)
	}
}

func TestRestoreCommandRefusals(t *testing.T) {
	writes := &fakeConsoleWrites{}
	for _, args := range [][]string{
		{"--from", "2026-11-13T12:00:00Z", "--to", "2026-11-13T11:00:00Z"},
		{"--partitions", "x"},
		{"--partitions", ""},
	} {
		if _, _, err := runRestore(writes, nil, "", append([]string{"kafka-a", "kafka3o-m10-verify", "--yes"}, args...)...); err == nil || len(writes.sent) != 0 {
			t.Fatalf("%v: %v", args, err)
		}
	}
	if _, _, err := runRestore(writes, nil, "n\n", "kafka-a", "kafka3o-m10-verify"); !errors.Is(err, console.ErrCanceled) || len(writes.sent) != 0 {
		t.Fatalf("cancel: %v", err)
	}
	if _, _, err := runRestore(writes, nil, "", "kafka-b", "kafka3o-m10-verify", "--yes"); !errors.Is(err, ErrReasonRequired) || len(writes.sent) != 0 {
		t.Fatalf("production: %v", err)
	}
	for code, want := range map[string]string{
		"restore_in_progress": "refused: A restore of this topic is already running.",
		"nothing_to_restore":  "refused: This backup has not stored any messages yet.",
	} {
		refused := &fakeConsoleWrites{errors: map[string]error{restorePath: console.Problem{Status: 409, Code: code}}}
		if _, _, err := runRestore(refused, nil, "", "kafka-a", "kafka3o-m10-verify", "--yes"); err == nil || err.Error() != want {
			t.Fatalf("%s: %v", code, err)
		}
	}
}
