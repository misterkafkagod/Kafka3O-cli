package cli

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
)

func TestDescribeTotalLag(t *testing.T) {
	for lag, want := range map[groups.Lag]string{
		{Kind: "known", Lag: "9007199254741000"}: "9007199254741000",
		{Kind: "unknown", Partitions: 1}:         "unknown (1 partition)",
		{Kind: "unknown", Partitions: 3}:         "unknown (3 partitions)",
		{Kind: "none"}:                           "none",
	} {
		if got := describeTotalLag(lag); got != want {
			t.Errorf("%+v: %q", lag, got)
		}
	}
}
