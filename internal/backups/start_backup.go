package backups

import (
	"context"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// StartBackup starts a continuous backup of a topic through the Console.
type StartBackup interface {
	Execute(ctx context.Context, session console.Session, cluster, topic string, settings *Settings, reason string) (Backup, error)
}

type startBackup struct {
	send console.SendJSON
}

func NewStartBackup(send console.SendJSON) StartBackup {
	return startBackup{send: send}
}

type startBackupBody struct {
	Topic    string    `json:"topic"`
	Settings *Settings `json:"settings,omitempty"`
}

// Execute sends only the chosen settings (none = the Gateway's defaults) and requires the created backup's topic.
func (s startBackup) Execute(ctx context.Context, session console.Session, cluster, topic string, settings *Settings, reason string) (Backup, error) {
	var created Backup
	if err := s.send.Execute(ctx, session, http.MethodPost, clusters.GatewayPath(cluster, "/api/v1/backups"), startBackupBody{Topic: topic, Settings: settings}, reason, &created); err != nil {
		return Backup{}, err
	}
	if created.Topic != topic {
		return Backup{}, fmt.Errorf("%w: the Gateway did not confirm the backup of %s", console.ErrOutcomeUnknown, topic)
	}
	created.Cluster = cluster
	return created, nil
}
