package cli

import (
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
)

// describeTotalLag is the total-lag text of the UI's consumer group list.
func describeTotalLag(lag groups.Lag) string {
	switch lag.Kind {
	case "known":
		return lag.Lag
	case "unknown":
		if lag.Partitions == 1 {
			return "unknown (1 partition)"
		}
		return fmt.Sprintf("unknown (%d partitions)", lag.Partitions)
	default:
		return "none"
	}
}
