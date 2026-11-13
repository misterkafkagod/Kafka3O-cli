package cli

import (
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

func TestTopicReadError(t *testing.T) {
	if topicReadError("c", "t", nil) != nil || !errors.Is(topicReadError("c", "t", console.ErrSignedOut), console.ErrSignedOut) {
		t.Fatal("nil or signed out changed")
	}
	var notFound topics.ErrTopicNotFound
	if err := topicReadError("kafka-a", "nope", console.Problem{Status: 404, Code: "not_found"}); !errors.As(err, &notFound) || ExitCode(err) != ExitUsage {
		t.Fatalf("not found: %v", err)
	}
	err := topicReadError("kafka-b", "t", console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"})
	if ExitCode(err) != ExitClusterFail || err.Error() != "kafka-b: Gateway unreachable: The Gateway could not be reached: at least one cluster could not answer" {
		t.Fatalf("unreachable: %v", err)
	}
}
