package connect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// RunConnectorAction asks the cluster's Gateway to pause, resume or restart a connector, or restart one task.
// Connect applies the action asynchronously.
type RunConnectorAction interface {
	Execute(ctx context.Context, session console.Session, cluster string, action ConnectorAction, reason string) error
}

type runConnectorAction struct {
	send console.SendJSON
}

func NewRunConnectorAction(send console.SendJSON) RunConnectorAction {
	return runConnectorAction{send: send}
}

// Execute requires the Gateway's echo of the connector, action and task, like the request it answers.
func (r runConnectorAction) Execute(ctx context.Context, session console.Session, cluster string, action ConnectorAction, reason string) error {
	path := "/api/v1/connect/connectors/" + url.PathEscape(action.Connector) + "/" + action.Kind
	if action.Kind == ActionRestartTask {
		path = fmt.Sprintf("/api/v1/connect/connectors/%s/tasks/%d/restart", url.PathEscape(action.Connector), action.Task)
	}
	var answer struct {
		Connector string `json:"connector"`
		Action    string `json:"action"`
		Task      *int32 `json:"task"`
	}
	err := r.send.Execute(ctx, session, http.MethodPost, clusters.GatewayPath(cluster, path), nil, reason, &answer)
	var problem console.Problem
	if errors.As(err, &problem) && problem.Code == "not_found" {
		return ErrConnectorNotFound{Cluster: cluster, Action: action}
	}
	if err != nil {
		return err
	}
	if answer.Connector != action.Connector || answer.Action != action.Kind || (action.Kind == ActionRestartTask) != (answer.Task != nil) ||
		(answer.Task != nil && *answer.Task != action.Task) {
		return fmt.Errorf("%w: the Gateway did not confirm the %s of %s", console.ErrOutcomeUnknown, action.Kind, action.Connector)
	}
	return nil
}
