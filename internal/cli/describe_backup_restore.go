package cli

import (
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// describeBackupRestore words a running, finished or failed restore; "" when there is none.
func describeBackupRestore(restore *topics.BackupRestore) string {
	switch {
	case restore == nil:
		return ""
	case restore.Finished:
		return fmt.Sprintf("last restore finished: %d records restored", restore.Total)
	case restore.Error != "":
		return "restore failed: " + messages.EscapeTerminalText(restore.Error)
	default:
		return fmt.Sprintf("restoring %d of %d records", restore.Restored, restore.Total)
	}
}
