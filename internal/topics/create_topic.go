package topics

import (
	"context"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// NewTopic is what the UI's create form sends.
type NewTopic struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
}

// CreateTopic creates a topic through the Console after checking the request.
type CreateTopic interface {
	Execute(ctx context.Context, session console.Session, cluster string, topic NewTopic, reason string) error
}

type createTopic struct {
	send console.SendJSON
}

func NewCreateTopic(send console.SendJSON) CreateTopic {
	return createTopic{send: send}
}

// Execute checks the echo like the UI: a Gateway answer for another topic means the outcome is not confirmed.
func (c createTopic) Execute(ctx context.Context, session console.Session, cluster string, topic NewTopic, reason string) error {
	var created NewTopic
	if err := c.send.Execute(ctx, session, http.MethodPost, clusters.GatewayPath(cluster, "/api/v1/topics"), topic, reason, &created); err != nil {
		return err
	}
	if created != topic {
		return fmt.Errorf("%w: the Gateway answered %+v", console.ErrOutcomeUnknown, created)
	}
	return nil
}
