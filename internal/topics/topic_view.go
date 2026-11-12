package topics

import "time"

// TopicView is everything `kafka3o topic` shows about one topic. Groups and Backup carry their own error when
// they could not be read; the topic itself could.
type TopicView struct {
	Cluster    string           `json:"cluster"`
	Name       string           `json:"name"`
	Partitions []TopicPartition `json:"partitions"`
	Config     []TopicConfig    `json:"config"`
	Groups     TopicGroups      `json:"consumerGroups"`
	Backup     TopicBackup      `json:"backup"`
}

// TopicPartition offsets are exact integers, as the Gateway sends them.
type TopicPartition struct {
	ID          int32   `json:"id"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	StartOffset int64   `json:"startOffset"`
	EndOffset   int64   `json:"endOffset"`
}

type TopicConfig struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Sensitive bool   `json:"sensitive"`
}

// TopicGroups are the consumer groups reading the topic; Unresolved counts groups the Gateway could not describe,
// which may or may not read it.
type TopicGroups struct {
	Error      string          `json:"error,omitempty"`
	Groups     []ConsumerGroup `json:"groups"`
	Unresolved int             `json:"unresolved"`
}

// ConsumerGroup keeps only this topic's partitions. Offsets and lag are decimal strings, nil when unknown.
type ConsumerGroup struct {
	ID         string           `json:"id"`
	State      string           `json:"state"`
	Members    []any            `json:"members"`
	Topics     []string         `json:"topics"`
	Partitions []GroupPartition `json:"partitions"`
	Error      string           `json:"error,omitempty"`
}

type GroupPartition struct {
	Topic           string  `json:"topic"`
	Partition       int32   `json:"partition"`
	CommittedOffset *string `json:"committedOffset"`
	EndOffset       *string `json:"endOffset"`
	Lag             *string `json:"lag"`
	Status          string  `json:"status"`
}

// TopicBackup is the topic's backup, if any. NotConfigured means the cluster has no backup storage.
type TopicBackup struct {
	Error         string          `json:"error,omitempty"`
	NotConfigured bool            `json:"notConfigured,omitempty"`
	Backup        *BackupSnapshot `json:"backup"`
}

type BackupSnapshot struct {
	State    string          `json:"state"`
	Error    string          `json:"error,omitempty"`
	Location string          `json:"location"`
	Storage  *BackupStorage  `json:"storage,omitempty"`
	Settings *BackupSettings `json:"settings,omitempty"`
	Restore  *BackupRestore  `json:"restore,omitempty"`
}

type BackupStorage struct {
	Objects     int        `json:"objects"`
	StoredBytes int64      `json:"storedBytes"`
	LastWrite   *time.Time `json:"lastWrite"`
}

type BackupSettings struct {
	StartFrom      string `json:"startFrom"`
	Compression    string `json:"compression"`
	RecordsPerFile int    `json:"recordsPerFile"`
}

type BackupRestore struct {
	State    string `json:"state"`
	Restored int64  `json:"restored"`
	Total    int64  `json:"total"`
	Finished bool   `json:"finished"`
	Error    string `json:"error,omitempty"`
}

// ErrTopicNotFound means the cluster answered that the topic does not exist.
type ErrTopicNotFound struct {
	Cluster, Topic string
}

func (e ErrTopicNotFound) Error() string {
	return "topic " + e.Topic + " was not found on " + e.Cluster
}

// ErrClusterRead is a cluster that could not answer for the topic itself, classified like the cluster list.
type ErrClusterRead struct {
	Cluster, Status, Reason string
}

func (e ErrClusterRead) Error() string {
	return e.Cluster + ": " + e.Status + ": " + e.Reason
}
