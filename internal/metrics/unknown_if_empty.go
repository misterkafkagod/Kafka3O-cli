package metrics

// unknownIfEmpty shows a value Kafka did not report as "unknown", so it is never mistaken for 0.
func unknownIfEmpty(text string) string {
	if text == "" {
		return "unknown"
	}
	return text
}
