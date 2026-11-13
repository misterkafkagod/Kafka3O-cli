// Package metrics reads a cluster's Kafka metrics through the Console and writes them as text or the UI's CSV.
package metrics

import "encoding/json"

// ClusterMetrics mirrors the Gateway's /api/v1/metrics document. Numbers stay json.Number and times stay the
// strings the Gateway sent, so exports copy them exactly; a nil pointer is a value Kafka did not report.
type ClusterMetrics struct {
	ReadAt         string          `json:"readAt"`
	LogDirs        LogDirs         `json:"logDirs"`
	Partitions     Partitions      `json:"partitions"`
	LargestTopics  LargestTopics   `json:"largestTopics"`
	EndOffsets     EndOffsets      `json:"endOffsets"`
	Quorum         *Quorum         `json:"quorum"`
	Brokers        *Brokers        `json:"brokers"`
	ConsumerGroups *ConsumerGroups `json:"consumerGroups"`
	Connect        *Connect        `json:"connect"`
	Backups        *Backups        `json:"backups"`
}

type LogDirs struct {
	Error string   `json:"error"`
	Dirs  []LogDir `json:"dirs"`
}

type LogDir struct {
	Broker      json.Number  `json:"broker"`
	Dir         string       `json:"dir"`
	TotalBytes  *json.Number `json:"totalBytes"`
	UsableBytes *json.Number `json:"usableBytes"`
	KafkaBytes  *json.Number `json:"kafkaBytes"`
	Error       string       `json:"error"`
}

type Partitions struct {
	Error            string       `json:"error"`
	Topics           *json.Number `json:"topics"`
	Partitions       *json.Number `json:"partitions"`
	UnderReplicated  *json.Number `json:"underReplicated"`
	Offline          *json.Number `json:"offline"`
	BelowMinISR      *json.Number `json:"belowMinIsr"`
	MinISRError      string       `json:"minIsrError"`
	UnreadableTopics []string     `json:"unreadableTopics"`
}

type LargestTopics struct {
	Error  string `json:"error"`
	Topics []struct {
		Topic string      `json:"topic"`
		Bytes json.Number `json:"bytes"`
	} `json:"topics"`
}

type EndOffsets struct {
	Error  string `json:"error"`
	Topics []struct {
		Topic     string  `json:"topic"`
		EndOffset *string `json:"endOffset"`
		Error     string  `json:"error"`
	} `json:"topics"`
}

type Quorum struct {
	Error         string       `json:"error"`
	LeaderID      *json.Number `json:"leaderId"`
	LeaderEpoch   json.Number  `json:"leaderEpoch"`
	HighWatermark json.Number  `json:"highWatermark"`
	Replicas      []struct {
		ID           json.Number  `json:"id"`
		Role         string       `json:"role"`
		LogEndOffset json.Number  `json:"logEndOffset"`
		Lag          *json.Number `json:"lag"`
		LastFetch    *string      `json:"lastFetch"`
	} `json:"replicas"`
}

type Brokers struct {
	Error   string `json:"error"`
	Brokers []struct {
		Broker              json.Number `json:"broker"`
		Leaders             json.Number `json:"leaders"`
		Replicas            json.Number `json:"replicas"`
		NonPreferredLeaders json.Number `json:"nonPreferredLeaders"`
	} `json:"brokers"`
}

type StateCount struct {
	State string      `json:"state"`
	Count json.Number `json:"count"`
}

type ConsumerGroups struct {
	Error  string       `json:"error"`
	States []StateCount `json:"states"`
	Groups []struct {
		Group             string      `json:"group"`
		State             string      `json:"state"`
		Members           json.Number `json:"members"`
		Lag               *string     `json:"lag"`
		MaxPartitionLag   *string     `json:"maxPartitionLag"`
		UnknownPartitions json.Number `json:"unknownPartitions"`
		Error             string      `json:"error"`
	} `json:"groups"`
}

type Connect struct {
	Error         string       `json:"error"`
	NotConfigured bool         `json:"notConfigured"`
	Connectors    []StateCount `json:"connectors"`
	Tasks         []StateCount `json:"tasks"`
	Failures      []struct {
		Connector string       `json:"connector"`
		Task      *json.Number `json:"task"`
		Error     string       `json:"error"`
	} `json:"failures"`
}

type Backups struct {
	Error         string `json:"error"`
	NotConfigured bool   `json:"notConfigured"`
	Backups       []struct {
		Topic         string       `json:"topic"`
		State         string       `json:"state"`
		Error         string       `json:"error"`
		StorageListed bool         `json:"storageListed"`
		Objects       *json.Number `json:"objects"`
		StoredBytes   *json.Number `json:"storedBytes"`
		LastWrite     *string      `json:"lastWrite"`
		Restoring     bool         `json:"restoring"`
	} `json:"backups"`
}
