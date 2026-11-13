package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ReadMetrics reads one cluster's metrics document once, returning it unchanged (for JSON output) and typed.
type ReadMetrics interface {
	Execute(ctx context.Context, session console.Session, cluster string) (json.RawMessage, ClusterMetrics, error)
}

type readMetrics struct {
	get console.GetJSON
}

func NewReadMetrics(get console.GetJSON) ReadMetrics {
	return readMetrics{get: get}
}

func (r readMetrics) Execute(ctx context.Context, session console.Session, cluster string) (json.RawMessage, ClusterMetrics, error) {
	var raw json.RawMessage
	if err := r.get.Execute(ctx, session, clusters.GatewayPath(cluster, "/api/v1/metrics"), &raw); err != nil {
		return nil, ClusterMetrics{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var typed ClusterMetrics
	if err := decoder.Decode(&typed); err != nil {
		return nil, ClusterMetrics{}, fmt.Errorf("%w: the metrics are not the expected JSON: %v", console.ErrConsoleUnavailable, err)
	}
	return raw, typed, nil
}
