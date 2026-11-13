package topics

// acceptedTopicChange is the Gateway's confirmation of a partition change: {"topic": …, "status": "accepted"}.
type acceptedTopicChange struct {
	Topic  string `json:"topic"`
	Status string `json:"status"`
}
