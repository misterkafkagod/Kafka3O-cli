package backups

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// StartRestore appends a topic's backed-up messages (all, or the selection) to the topic itself.
type StartRestore interface {
	Execute(ctx context.Context, session console.Session, cluster, topic string, selection *Selection, reason string) (Restore, error)
}

type startRestore struct {
	send console.SendJSON
}

func NewStartRestore(send console.SendJSON) StartRestore {
	return startRestore{send: send}
}

// Execute sends no body to restore everything, and requires the started restore's topic.
func (s startRestore) Execute(ctx context.Context, session console.Session, cluster, topic string, selection *Selection, reason string) (Restore, error) {
	var body any
	if selection != nil {
		body = selection
	}
	var started Restore
	if err := s.send.Execute(ctx, session, http.MethodPost, clusters.GatewayPath(cluster, "/api/v1/backups/"+url.PathEscape(topic)+"/restore"), body, reason, &started); err != nil {
		return Restore{}, err
	}
	if started.Topic != topic || started.RestoreID == "" {
		return Restore{}, fmt.Errorf("%w: the Gateway did not confirm the restore of %s", console.ErrOutcomeUnknown, topic)
	}
	return started, nil
}
