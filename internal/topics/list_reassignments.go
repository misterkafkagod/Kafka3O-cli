package topics

import (
	"context"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// Reassignment is a partition move Kafka is still carrying out.
type Reassignment struct {
	Partition        int32   `json:"partition"`
	Replicas         []int32 `json:"replicas"`
	AddingReplicas   []int32 `json:"addingReplicas"`
	RemovingReplicas []int32 `json:"removingReplicas"`
}

// ListReassignments reads a topic's partition moves in progress.
type ListReassignments interface {
	Execute(ctx context.Context, session console.Session, cluster, topic string) ([]Reassignment, error)
}

type listReassignments struct {
	get console.GetJSON
}

func NewListReassignments(get console.GetJSON) ListReassignments {
	return listReassignments{get: get}
}

func (l listReassignments) Execute(ctx context.Context, session console.Session, cluster, topic string) ([]Reassignment, error) {
	var moves []Reassignment
	if err := l.get.Execute(ctx, session, clusters.GatewayPath(cluster, "/api/v1/topics/"+url.PathEscape(topic)+"/reassignments"), &moves); err != nil {
		return nil, err
	}
	if moves == nil {
		moves = []Reassignment{}
	}
	return moves, nil
}
