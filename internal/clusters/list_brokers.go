package clusters

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListBrokers reads the brokers of one cluster or of every accessible cluster.
type ListBrokers interface {
	Execute(ctx context.Context, session console.Session, cluster string) ([]Broker, []ClusterFailure, error)
}

type listBrokers struct {
	accessible ListAccessibleClusters
	get        console.GetJSON
}

func NewListBrokers(accessible ListAccessibleClusters, get console.GetJSON) ListBrokers {
	return listBrokers{accessible: accessible, get: get}
}

func (l listBrokers) Execute(ctx context.Context, session console.Session, cluster string) ([]Broker, []ClusterFailure, error) {
	perCluster, failures, err := ReadAcrossClusters(ctx, session, l.accessible, cluster, func(ctx context.Context, cluster AccessibleCluster) ([]Broker, error) {
		var answer struct {
			ControllerID int32 `json:"controllerId"`
			Brokers      []struct {
				ID   int32  `json:"id"`
				Host string `json:"host"`
				Port int32  `json:"port"`
			} `json:"brokers"`
		}
		if err := l.get.Execute(ctx, session, GatewayPath(cluster.ID, "/api/v1/cluster"), &answer); err != nil {
			return nil, err
		}
		brokers := make([]Broker, 0, len(answer.Brokers))
		for _, broker := range answer.Brokers {
			brokers = append(brokers, Broker{Cluster: cluster.ID, ID: broker.ID, Host: broker.Host, Port: broker.Port, Controller: broker.ID == answer.ControllerID})
		}
		return brokers, nil
	})
	if err != nil {
		return nil, nil, err
	}
	all := []Broker{}
	for _, brokers := range perCluster {
		all = append(all, brokers...)
	}
	return all, failures, nil
}
