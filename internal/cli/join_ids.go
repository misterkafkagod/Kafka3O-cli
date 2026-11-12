package cli

import (
	"strconv"
	"strings"
)

// joinIDs writes broker or partition IDs as "1,2,3".
func joinIDs(values []int32) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.Itoa(int(value))
	}
	return strings.Join(parts, ",")
}
