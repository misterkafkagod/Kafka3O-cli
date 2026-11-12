package topics

import (
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestSectionReason(t *testing.T) {
	for err, want := range map[error]string{
		console.Problem{Status: 503, Code: "connect_unreachable", Title: "Kafka Connect is unreachable"}:           "Kafka Connect is unreachable",
		console.Problem{Status: 503, Code: "cluster_unreachable", Title: "Cluster unreachable", Detail: "timeout"}: "timeout",
		console.Problem{Status: 500}: "Internal Server Error (500)",
		errors.New("plain"):          "plain",
	} {
		if got := SectionReason(err); got != want {
			t.Errorf("%v: %q, want %q", err, got, want)
		}
	}
}
