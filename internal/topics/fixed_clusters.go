package topics

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// fixedClusters answers with a cluster list already read, so one command asks the Console for it only once.
type fixedClusters []clusters.AccessibleCluster

func (f fixedClusters) Execute(context.Context, console.Session) ([]clusters.AccessibleCluster, error) {
	return f, nil
}
