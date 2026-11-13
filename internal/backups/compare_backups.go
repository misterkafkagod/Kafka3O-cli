package backups

import "strings"

// CompareBackups orders backups by topic.
func CompareBackups(a, b Backup) int {
	return strings.Compare(a.Topic, b.Topic)
}
