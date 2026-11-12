package clusters

import (
	"errors"
	"fmt"
	"testing"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

func TestClassifyClusterError(t *testing.T) {
	for _, tt := range []struct {
		err            error
		status, reason string
	}{
		{console.Problem{Status: 503, Code: "gateway_unavailable", Title: "The Gateway could not be reached"}, StatusGatewayUnreachable, "The Gateway could not be reached"},
		{console.Problem{Status: 503, Code: "cluster_unreachable", Title: "Cluster unreachable", Detail: "kafka: dial tcp 10.0.0.1:9092: i/o timeout"}, StatusClusterUnreachable, "kafka: dial tcp 10.0.0.1:9092: i/o timeout"},
		{console.Problem{Status: 403, Code: "forbidden", Title: "Forbidden", Detail: "Not permitted: no_role"}, StatusAccessDenied, "Not permitted: no_role"},
		{console.Problem{Status: 503, Code: "authorization_unavailable", Title: "Authorization is unavailable"}, StatusAuthorizationUnavailable, "Authorization is unavailable"},
		{console.Problem{Status: 500, Code: "internal", Title: "Internal error"}, StatusUnavailable, "Internal error (500 internal)"},
		{fmt.Errorf("%w: dial tcp 127.0.0.1:5173: connection refused", console.ErrConsoleUnavailable), StatusGatewayUnreachable, "the console could not be reached or gave an unexpected answer: dial tcp 127.0.0.1:5173: connection refused"},
		{errors.New("something else"), StatusUnavailable, "something else"},
	} {
		status, reason := ClassifyClusterError(tt.err)
		if status != tt.status || reason != tt.reason {
			t.Errorf("%v: %s %q, want %s %q", tt.err, status, reason, tt.status, tt.reason)
		}
	}
}
