package cli

import (
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

func TestDescribeError(t *testing.T) {
	err := describeError(topics.ErrClusterRead{Cluster: "kafka-b", Status: "gateway_unreachable", Reason: "The Gateway could not be reached"})
	if ExitCode(err) != ExitClusterFail || err.Error() != "kafka-b: Gateway unreachable: The Gateway could not be reached: at least one cluster could not answer" {
		t.Fatalf("%v", err)
	}
	other := errors.New("x")
	if describeError(other) != other {
		t.Fatal("other errors changed")
	}
}
