package cli

// orUnknown prints a value the Gateway may not know (an offset or a lag) as "unknown", never as 0.
func orUnknown(value *string) string {
	if value == nil {
		return "unknown"
	}
	return *value
}
