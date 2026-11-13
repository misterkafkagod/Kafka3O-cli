package cli

import (
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// writeError turns a refused or failed change into the command's error: a denial explains why (exit 1), another
// refusal shows the Gateway's reason (exit 1), an unreachable cluster exits 2; signed out, canceled and "may have
// been applied" keep their own exit codes.
func writeError(cluster string, err error) error {
	var problem console.Problem
	if err == nil || !errors.As(err, &problem) {
		return err
	}
	switch {
	case problem.Status == 403 && problem.Code == "forbidden":
		return fmt.Errorf("denied: %s", describeAccessDenial(problem.Detail))
	case problem.Status >= 500:
		status, reason := clusters.ClassifyClusterError(err)
		return fmt.Errorf("%s: %s: %w", cluster, describeClusterStatus(clusters.ClusterHealth{Status: status, Error: reason}), ErrClusterFailed)
	default:
		message := problem.Detail
		if message == "" {
			message = problem.Title
		}
		if message == "" {
			message = problem.Error()
		}
		return fmt.Errorf("refused: %s", message)
	}
}
