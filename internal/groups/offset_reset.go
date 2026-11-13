package groups

// ResetTarget is where a reset moves the group's committed offsets: "earliest", "latest" or a timestamp (Unix ms).
type ResetTarget struct {
	Kind      string
	Timestamp int64
}

// ResetPreview is the Gateway's preview: per partition the offset committed now (nil when none) and the new one.
// Offsets are exact decimal strings.
type ResetPreview struct {
	Group      string `json:"group"`
	Topic      string `json:"topic"`
	Partitions []struct {
		Partition     int32   `json:"partition"`
		CurrentOffset *string `json:"currentOffset"`
		TargetOffset  string  `json:"targetOffset"`
	} `json:"partitions"`
}

// AppliedReset is what the Gateway committed.
type AppliedReset struct {
	Group      string `json:"group"`
	Topic      string `json:"topic"`
	Partitions []struct {
		Partition       int32  `json:"partition"`
		CommittedOffset string `json:"committedOffset"`
	} `json:"partitions"`
}
