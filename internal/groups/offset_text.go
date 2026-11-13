package groups

// offsetText prints an exact decimal offset or lag, or "unknown" when Kafka did not report it.
func offsetText(value *string) string {
	if value == nil {
		return "unknown"
	}
	return *value
}
