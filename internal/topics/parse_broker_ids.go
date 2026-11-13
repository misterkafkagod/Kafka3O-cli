package topics

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseBrokerIDs reads "1,2,3" into distinct non-negative broker ids, in the given order (the first is preferred).
func ParseBrokerIDs(text string) ([]int32, error) {
	seen := map[int32]bool{}
	ids := []int32{}
	for _, part := range strings.Split(text, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 32)
		if err != nil || id < 0 {
			return nil, fmt.Errorf("replicas must be broker ids separated by commas, such as 1,2,3, not %q", text)
		}
		if seen[int32(id)] {
			return nil, fmt.Errorf("broker %d is listed twice in %q", id, text)
		}
		seen[int32(id)] = true
		ids = append(ids, int32(id))
	}
	return ids, nil
}
