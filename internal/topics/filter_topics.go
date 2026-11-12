package topics

import "strings"

// FilterTopics keeps topics whose name contains the text (case-insensitive; empty keeps all) and hides internal
// topics unless includeInternal is set.
func FilterTopics(topics []Topic, text string, includeInternal bool) []Topic {
	needle := strings.ToLower(strings.TrimSpace(text))
	kept := []Topic{}
	for _, topic := range topics {
		if (topic.Internal && !includeInternal) || !strings.Contains(strings.ToLower(topic.Name), needle) {
			continue
		}
		kept = append(kept, topic)
	}
	return kept
}
