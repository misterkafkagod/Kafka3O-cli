package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// backupError is a port of the UI's describeBackupError for refusals that have their own wording; denials,
// unreachable clusters and anything else go through writeError.
func backupError(cluster, topic string, err error) error {
	var problem console.Problem
	if !errors.As(err, &problem) || problem.Status == 403 || problem.Status >= 500 {
		return writeError(cluster, err)
	}
	switch problem.Code {
	case "restore_in_progress":
		return errors.New("refused: A restore of this topic is already running.")
	case "nothing_to_restore":
		return errors.New("refused: This backup has not stored any messages yet.")
	case "not_found":
		return fmt.Errorf("topic %s has no backup on %s", topic, cluster)
	case "backup_exists":
		return errors.New("refused: This topic is already backed up.")
	case "backups_not_configured":
		return errors.New("refused: Backups are not configured for this cluster.")
	case "invalid_backup_settings", "invalid_restore_filter":
		if problem.Detail != "" {
			return fmt.Errorf("refused: %s%s.", strings.ToUpper(problem.Detail[:1]), problem.Detail[1:])
		}
	}
	return writeError(cluster, err)
}
