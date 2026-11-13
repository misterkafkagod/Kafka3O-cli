package topics

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// DeleteTopic deletes a topic through the Console.
type DeleteTopic interface {
	Execute(ctx context.Context, session console.Session, cluster, topic, reason string) error
}

type deleteTopic struct {
	send console.SendJSON
}

func NewDeleteTopic(send console.SendJSON) DeleteTopic {
	return deleteTopic{send: send}
}

// Execute requires the Gateway's {"name": topic, "status": "accepted"} like the UI does.
func (d deleteTopic) Execute(ctx context.Context, session console.Session, cluster, topic, reason string) error {
	var answer struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := d.send.Execute(ctx, session, http.MethodDelete, clusters.GatewayPath(cluster, "/api/v1/topics/"+url.PathEscape(topic)), nil, reason, &answer); err != nil {
		return err
	}
	if answer.Name != topic || answer.Status != "accepted" {
		return fmt.Errorf("%w: the Gateway did not confirm deleting %s", console.ErrOutcomeUnknown, topic)
	}
	return nil
}
