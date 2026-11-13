package clusters

import (
	"context"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ConfigurationChange is the Gateway's change body: operation "set" with a value, or "reset" without one.
type ConfigurationChange struct {
	Name      string  `json:"name"`
	Operation string  `json:"operation"`
	Value     *string `json:"value"`
}

// ChangeConfiguration sets or resets one setting of a topic or broker through the Console.
type ChangeConfiguration interface {
	Execute(ctx context.Context, session console.Session, cluster string, resource ConfigurationResource, change ConfigurationChange, reason string) error
}

type changeConfiguration struct {
	send console.SendJSON
}

func NewChangeConfiguration(send console.SendJSON) ChangeConfiguration {
	return changeConfiguration{send: send}
}

// Execute requires the Gateway's {"name": setting, "status": "accepted"}, like the UI.
func (c changeConfiguration) Execute(ctx context.Context, session console.Session, cluster string, resource ConfigurationResource, change ConfigurationChange, reason string) error {
	var answer struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	if err := c.send.Execute(ctx, session, http.MethodPost, GatewayPath(cluster, ConfigurationPath(resource)), change, reason, &answer); err != nil {
		return err
	}
	if answer.Status != "accepted" || answer.Name != change.Name {
		return fmt.Errorf("%w: the Gateway did not confirm changing %s", console.ErrOutcomeUnknown, change.Name)
	}
	return nil
}
