package backups

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// RunBackupAction pauses, resumes or deletes a topic's backup job; deleting keeps the stored objects.
type RunBackupAction interface {
	Execute(ctx context.Context, session console.Session, cluster, topic, kind, reason string) error
}

type runBackupAction struct {
	send console.SendJSON
}

func NewRunBackupAction(send console.SendJSON) RunBackupAction {
	return runBackupAction{send: send}
}

// Execute requires the Gateway's {topic, action} echo.
func (r runBackupAction) Execute(ctx context.Context, session console.Session, cluster, topic, kind, reason string) error {
	method, path := http.MethodPost, "/api/v1/backups/"+url.PathEscape(topic)+"/"+kind
	if kind == ActionDelete {
		method, path = http.MethodDelete, "/api/v1/backups/"+url.PathEscape(topic)
	}
	var answer struct {
		Topic  string `json:"topic"`
		Action string `json:"action"`
	}
	if err := r.send.Execute(ctx, session, method, clusters.GatewayPath(cluster, path), nil, reason, &answer); err != nil {
		return err
	}
	if answer.Topic != topic || answer.Action != kind {
		return fmt.Errorf("%w: the Gateway did not confirm the %s of the backup of %s", console.ErrOutcomeUnknown, kind, topic)
	}
	return nil
}
