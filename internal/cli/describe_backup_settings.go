package cli

import (
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// describeBackupSettings words a backup's effective settings like the UI; "" when they could not be read.
func describeBackupSettings(settings *topics.BackupSettings) string {
	if settings == nil {
		return ""
	}
	start := map[string]string{"earliest": "from the oldest message", "latest": "messages from when the backup started"}[settings.StartFrom]
	if start == "" {
		start = messages.EscapeTerminalText(settings.StartFrom)
	}
	records := "no record limit"
	if settings.RecordsPerFile > 0 {
		records = fmt.Sprintf("up to %d records per file", settings.RecordsPerFile)
	}
	return start + ", " + messages.EscapeTerminalText(settings.Compression) + ", " + records
}
