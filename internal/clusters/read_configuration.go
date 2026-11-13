package clusters

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ReadConfiguration reads a topic's or a broker's configuration through the Console.
type ReadConfiguration interface {
	Execute(ctx context.Context, session console.Session, cluster string, resource ConfigurationResource) ([]Setting, error)
}

type readConfiguration struct {
	get console.GetJSON
}

func NewReadConfiguration(get console.GetJSON) ReadConfiguration {
	return readConfiguration{get: get}
}

func (r readConfiguration) Execute(ctx context.Context, session console.Session, cluster string, resource ConfigurationResource) ([]Setting, error) {
	var settings []Setting
	if err := r.get.Execute(ctx, session, GatewayPath(cluster, ConfigurationPath(resource)), &settings); err != nil {
		return nil, err
	}
	for i := range settings {
		if settings[i].Sensitive {
			settings[i].Value = nil
		}
	}
	return settings, nil
}
