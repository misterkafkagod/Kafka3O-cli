package groups

// StatusLabel is a port of the UI's consumerLagStatusLabel.
func StatusLabel(status string) string {
	switch status {
	case "ok":
		return "Available"
	case "no_commit":
		return "No committed offset"
	case "out_of_range":
		return "Commit outside log range"
	case "unavailable":
		return "Unavailable"
	default:
		return "Unknown status"
	}
}
