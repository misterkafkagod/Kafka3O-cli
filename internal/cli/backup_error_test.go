package cli

import (
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestBackupError(t *testing.T) {
	for problem, want := range map[console.Problem]string{
		{Status: 409, Code: "restore_in_progress", Detail: "A restore of this topic is already running"}:                         "refused: A restore of this topic is already running.",
		{Status: 404, Code: "nothing_to_restore", Detail: "This topic's backup holds no stored objects yet"}:                     "refused: This backup has not stored any messages yet.",
		{Status: 404, Code: "not_found", Detail: "This topic has no backup"}:                                                     "topic orders has no backup on kafka-a",
		{Status: 409, Code: "backup_exists", Detail: "This topic already has a backup"}:                                          "refused: This topic is already backed up.",
		{Status: 404, Code: "backups_not_configured", Detail: "Backups are not configured for this cluster"}:                     "refused: Backups are not configured for this cluster.",
		{Status: 400, Code: "invalid_restore_filter", Detail: "the restore selection is invalid: choose at least one partition"}: "refused: The restore selection is invalid: choose at least one partition.",
		{Status: 409, Code: "connect_rejected", Detail: "Kafka Connect rejected the action; refresh and try again"}:              "refused: Kafka Connect rejected the action; refresh and try again",
		{Status: 403, Code: "forbidden", Detail: "Not permitted: change_reason_required"}:                                        "denied: Production changes need a recorded change reason.",
	} {
		if err := backupError("kafka-a", "orders", problem); err == nil || err.Error() != want || ExitCode(err) != ExitUsage {
			t.Errorf("%s: %v", problem.Code, err)
		}
	}
	unreachable := backupError("kafka-a", "orders", console.Problem{Status: 503, Code: "storage_unavailable", Detail: "The backup storage cannot be listed"})
	if ExitCode(unreachable) != ExitClusterFail {
		t.Errorf("storage: %v", unreachable)
	}
	if err := backupError("kafka-a", "orders", console.ErrOutcomeUnknown); !errors.Is(err, console.ErrOutcomeUnknown) {
		t.Errorf("unknown: %v", err)
	}
}
