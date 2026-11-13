package backups

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// RestoreOutcome is how a followed restore ended. Gone means it was no longer listed: the Gateway finishes a
// restore while listing backups, so another listing finished it.
type RestoreOutcome struct {
	Finished bool
	Gone     bool
	Restored int64
	Total    int64
	Error    string
}

// WaitForRestore follows one restore by listing the cluster's backups until the restore ends.
type WaitForRestore interface {
	Execute(ctx context.Context, session console.Session, cluster, topic, restoreID string, progress io.Writer) (RestoreOutcome, error)
}

type waitForRestore struct {
	list     ListBackups
	sleep    Sleep
	interval time.Duration
}

func NewWaitForRestore(list ListBackups, sleep Sleep, interval time.Duration) WaitForRestore {
	return waitForRestore{list: list, sleep: sleep, interval: interval}
}

// Execute writes a line whenever the progress text changes. A restore whose connector failed ends with its error;
// "progress unavailable" is shown and the wait continues. A cluster that cannot be listed ends the wait with
// topics.ErrClusterRead (the restore keeps running).
func (w waitForRestore) Execute(ctx context.Context, session console.Session, cluster, topic, restoreID string, progress io.Writer) (RestoreOutcome, error) {
	last := ""
	for {
		found, failures, err := w.list.Execute(ctx, session, cluster)
		if err != nil {
			return RestoreOutcome{}, err
		}
		if len(failures) > 0 {
			return RestoreOutcome{}, topics.ErrClusterRead{Cluster: cluster, Status: failures[0].Status, Reason: failures[0].Error}
		}
		var backup *Backup
		for i := range found[0].Backups {
			if found[0].Backups[i].Topic == topic {
				backup = &found[0].Backups[i]
			}
		}
		line := ""
		switch {
		case backup == nil || (backup.Restore == nil && !backup.Restoring):
			return RestoreOutcome{Gone: true}, nil
		case backup.Restore == nil:
			line = "restore running; progress not checked yet"
		case backup.Restore.RestoreID != "" && backup.Restore.RestoreID != restoreID:
			return RestoreOutcome{Gone: true}, nil
		case backup.Restore.Finished:
			return RestoreOutcome{Finished: true, Restored: backup.Restore.Restored, Total: backup.Restore.Total}, nil
		case backup.Restore.State == "FAILED":
			return RestoreOutcome{Error: backup.Restore.Error, Restored: backup.Restore.Restored, Total: backup.Restore.Total}, nil
		case backup.Restore.Error != "":
			line = backup.Restore.Error
		default:
			line = fmt.Sprintf("restoring %d of %d records", backup.Restore.Restored, backup.Restore.Total)
		}
		if line != last {
			fmt.Fprintln(progress, line)
			last = line
		}
		if err := w.sleep.Execute(ctx, w.interval); err != nil {
			return RestoreOutcome{}, err
		}
	}
}
