package topics

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// IncreasePartitions raises a topic's partition count to a new total through the Console.
type IncreasePartitions interface {
	Execute(ctx context.Context, session console.Session, cluster, topic string, total int, reason string) error
}

type increasePartitions struct {
	send console.SendJSON
}

func NewIncreasePartitions(send console.SendJSON) IncreasePartitions {
	return increasePartitions{send: send}
}

func (i increasePartitions) Execute(ctx context.Context, session console.Session, cluster, topic string, total int, reason string) error {
	var answer acceptedTopicChange
	path := clusters.GatewayPath(cluster, "/api/v1/topics/"+url.PathEscape(topic)+"/partitions")
	if err := i.send.Execute(ctx, session, http.MethodPost, path, map[string]int{"count": total}, reason, &answer); err != nil {
		return err
	}
	if answer.Status != "accepted" || answer.Topic != topic {
		return fmt.Errorf("%w: the Gateway did not confirm adding partitions to %s", console.ErrOutcomeUnknown, topic)
	}
	return nil
}
