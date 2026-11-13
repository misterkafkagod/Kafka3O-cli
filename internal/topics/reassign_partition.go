package topics

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ReassignPartition asks Kafka to move one partition's replicas to the given brokers.
type ReassignPartition interface {
	Execute(ctx context.Context, session console.Session, cluster, topic string, partition int32, replicas []int32, reason string) error
}

type reassignPartition struct {
	send console.SendJSON
}

func NewReassignPartition(send console.SendJSON) ReassignPartition {
	return reassignPartition{send: send}
}

func (r reassignPartition) Execute(ctx context.Context, session console.Session, cluster, topic string, partition int32, replicas []int32, reason string) error {
	var answer acceptedTopicChange
	path := clusters.GatewayPath(cluster, "/api/v1/topics/"+url.PathEscape(topic)+"/partitions/"+strconv.Itoa(int(partition))+"/replicas")
	if err := r.send.Execute(ctx, session, http.MethodPost, path, map[string][]int32{"replicas": replicas}, reason, &answer); err != nil {
		return err
	}
	if answer.Status != "accepted" || answer.Topic != topic {
		return fmt.Errorf("%w: the Gateway did not confirm moving %s partition %d", console.ErrOutcomeUnknown, topic, partition)
	}
	return nil
}
