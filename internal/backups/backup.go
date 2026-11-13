package backups

import "github.com/misterkafkagod/Kafka3O-cli/internal/topics"

// Backup is one topic's continuous backup job; the embedded snapshot keeps the Gateway's field names.
type Backup struct {
	Cluster   string `json:"cluster"`
	Topic     string `json:"topic"`
	Connector string `json:"connector"`
	// Restoring is true while a restore connector exists, also when its progress was not checked.
	Restoring bool `json:"restoring,omitempty"`
	topics.BackupSnapshot
}

// ClusterBackups is one cluster's backups. NotConfigured means the cluster has no backup storage.
type ClusterBackups struct {
	Cluster       string   `json:"cluster"`
	NotConfigured bool     `json:"notConfigured"`
	Backups       []Backup `json:"backups"`
}
