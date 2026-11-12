package clusters

import (
	"errors"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ClassifyClusterError names why a cluster could not answer, with the same categories as the UI's cluster cards,
// and returns the reason to show (the problem's detail, or its title).
func ClassifyClusterError(err error) (status, reason string) {
	var problem console.Problem
	if !errors.As(err, &problem) {
		if errors.Is(err, console.ErrConsoleUnavailable) {
			return StatusGatewayUnreachable, err.Error()
		}
		return StatusUnavailable, err.Error()
	}
	reason = problem.Detail
	if reason == "" {
		reason = problem.Title
	}
	switch {
	case problem.Code == "gateway_unavailable":
		return StatusGatewayUnreachable, reason
	case problem.Code == "cluster_unreachable":
		return StatusClusterUnreachable, reason
	case problem.Status == 403 && problem.Code == "forbidden":
		return StatusAccessDenied, reason
	case problem.Code == "authorization_unavailable":
		return StatusAuthorizationUnavailable, reason
	default:
		return StatusUnavailable, problem.Error()
	}
}
