package connect

import "fmt"

// Connector action kinds, as the Gateway names them.
const (
	ActionPause       = "pause"
	ActionResume      = "resume"
	ActionRestart     = "restart"
	ActionRestartTask = "restart-task"
)

// ConnectorAction is one lifecycle request; Task is used only by ActionRestartTask.
type ConnectorAction struct {
	Kind      string
	Connector string
	Task      int32
}

// ErrConnectorNotFound means the cluster's Connect has no such connector, or no such task.
type ErrConnectorNotFound struct {
	Cluster string
	Action  ConnectorAction
}

func (e ErrConnectorNotFound) Error() string {
	if e.Action.Kind == ActionRestartTask {
		return fmt.Sprintf("task %d of connector %s was not found on %s", e.Action.Task, e.Action.Connector, e.Cluster)
	}
	return fmt.Sprintf("connector %s was not found on %s", e.Action.Connector, e.Cluster)
}
