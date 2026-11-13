package backups

import "time"

// Selection narrows a restore to record timestamps in [From, To) and to some partitions; nil fields mean no limit.
type Selection struct {
	From       *time.Time `json:"from,omitempty"`
	To         *time.Time `json:"to,omitempty"`
	Partitions []int      `json:"partitions,omitempty"`
}

// Restore is a restore the Gateway started from the objects stored at that moment.
type Restore struct {
	Topic     string `json:"topic"`
	RestoreID string `json:"restoreId"`
	Objects   int    `json:"objects"`
	// Skipped counts stored objects written with another compression, which this restore does not read.
	Skipped int `json:"skipped"`
}
