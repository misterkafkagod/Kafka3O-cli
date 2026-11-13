package clusters

import (
	"context"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// BrokerSetting is one broker configuration entry. Value is nil when Kafka reports none, and always nil for a
// sensitive setting, so a secret can never be printed.
type BrokerSetting struct {
	Name      string  `json:"name"`
	Value     *string `json:"value"`
	Sensitive bool    `json:"sensitive"`
	ReadOnly  bool    `json:"readOnly"`
	Source    string  `json:"source"`
	Type      string  `json:"type"`
}

// ReadBrokerConfig reads one broker's configuration from its cluster's Gateway.
type ReadBrokerConfig interface {
	Execute(ctx context.Context, session console.Session, cluster string, broker int32) ([]BrokerSetting, error)
}

type readBrokerConfig struct {
	get console.GetJSON
}

func NewReadBrokerConfig(get console.GetJSON) ReadBrokerConfig {
	return readBrokerConfig{get: get}
}

func (r readBrokerConfig) Execute(ctx context.Context, session console.Session, cluster string, broker int32) ([]BrokerSetting, error) {
	var settings []BrokerSetting
	path := GatewayPath(cluster, "/api/v1/brokers/"+strconv.Itoa(int(broker))+"/config")
	if err := r.get.Execute(ctx, session, path, &settings); err != nil {
		return nil, err
	}
	for i := range settings {
		if settings[i].Sensitive {
			settings[i].Value = nil
		}
	}
	if settings == nil {
		settings = []BrokerSetting{}
	}
	return settings, nil
}
