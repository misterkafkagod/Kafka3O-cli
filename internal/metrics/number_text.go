package metrics

import "encoding/json"

// numberText writes a number exactly as the Gateway sent it, or "" when Kafka did not report one.
func numberText(number *json.Number) string {
	if number == nil {
		return ""
	}
	return number.String()
}
