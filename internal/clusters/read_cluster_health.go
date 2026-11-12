package clusters

import (
	"context"
	"errors"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ReadClusterHealth asks one cluster's Gateway (through the Console) for its cluster ID, controller and brokers.
type ReadClusterHealth interface {
	Execute(ctx context.Context, session console.Session, cluster AccessibleCluster) (ClusterHealth, error)
}

type readClusterHealth struct {
	get console.GetJSON
}

func NewReadClusterHealth(get console.GetJSON) ReadClusterHealth {
	return readClusterHealth{get: get}
}

// Execute returns an error only for a session that is over; any other failure becomes the cluster's status, so one
// cluster never hides the others.
func (r readClusterHealth) Execute(ctx context.Context, session console.Session, cluster AccessibleCluster) (ClusterHealth, error) {
	var answer struct {
		ClusterID    string `json:"clusterId"`
		ControllerID int32  `json:"controllerId"`
		Brokers      []any  `json:"brokers"`
	}
	err := r.get.Execute(ctx, session, GatewayPath(cluster.ID, "/api/v1/cluster"), &answer)
	if errors.Is(err, console.ErrSignedOut) {
		return ClusterHealth{}, err
	}
	if err != nil {
		return ClusterHealth{AccessibleCluster: cluster, Status: StatusUnavailable, Error: err.Error()}, nil
	}
	brokers, controller := len(answer.Brokers), answer.ControllerID
	return ClusterHealth{AccessibleCluster: cluster, Status: StatusHealthy, ClusterID: answer.ClusterID, ControllerID: &controller, Brokers: &brokers}, nil
}
