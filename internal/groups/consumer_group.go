// Package groups reads consumer groups and their offsets from every accessible cluster through the Console.
package groups

// ConsumerGroup is a group as a Gateway describes it, with the cluster it belongs to. Offsets and lag are exact
// decimal strings; nil means Kafka did not report them.
type ConsumerGroup struct {
	Cluster    string      `json:"cluster"`
	ID         string      `json:"id"`
	Protocol   string      `json:"protocol"`
	State      string      `json:"state"`
	Members    []Member    `json:"members"`
	Topics     []string    `json:"topics"`
	Partitions []Partition `json:"partitions"`
	Error      string      `json:"error,omitempty"`
}

type Member struct {
	ID         string  `json:"id"`
	ClientID   string  `json:"clientId"`
	ClientHost string  `json:"clientHost"`
	InstanceID *string `json:"instanceId,omitempty"`
}

type Partition struct {
	Topic           string  `json:"topic"`
	Partition       int32   `json:"partition"`
	CommittedOffset *string `json:"committedOffset"`
	StartOffset     *string `json:"startOffset"`
	EndOffset       *string `json:"endOffset"`
	Lag             *string `json:"lag"`
	Status          string  `json:"status"`
	Error           string  `json:"error,omitempty"`
}

// Lag is a group's total lag as the UI shows it: Kind "known" with the exact sum, "unknown" with the number of
// partitions whose lag Kafka did not report, or "none" for a group without partitions.
type Lag struct {
	Kind       string `json:"kind"`
	Lag        string `json:"lag,omitempty"`
	Partitions int    `json:"unknownPartitions,omitempty"`
}
