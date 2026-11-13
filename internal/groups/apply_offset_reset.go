package groups

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ApplyOffsetReset commits a preview's targets.
type ApplyOffsetReset interface {
	Execute(ctx context.Context, session console.Session, cluster string, preview ResetPreview, reason string) (AppliedReset, error)
}

type applyOffsetReset struct {
	send console.SendJSON
}

func NewApplyOffsetReset(send console.SendJSON) ApplyOffsetReset {
	return applyOffsetReset{send: send}
}

// Execute sends exactly the previewed targets with the offsets the preview saw, like the UI, so the Gateway
// refuses a reset whose basis has changed (stale_preview).
func (a applyOffsetReset) Execute(ctx context.Context, session console.Session, cluster string, preview ResetPreview, reason string) (AppliedReset, error) {
	type partition struct {
		Partition      int32   `json:"partition"`
		ExpectedOffset *string `json:"expectedOffset"`
		TargetOffset   string  `json:"targetOffset"`
	}
	body := struct {
		Topic      string      `json:"topic"`
		Partitions []partition `json:"partitions"`
	}{Topic: preview.Topic, Partitions: []partition{}}
	for _, p := range preview.Partitions {
		body.Partitions = append(body.Partitions, partition{Partition: p.Partition, ExpectedOffset: p.CurrentOffset, TargetOffset: p.TargetOffset})
	}
	var applied AppliedReset
	path := clusters.GatewayPath(cluster, "/api/v1/consumer-groups/"+url.PathEscape(preview.Group)+"/offset-reset")
	if err := a.send.Execute(ctx, session, http.MethodPost, path, body, reason, &applied); err != nil {
		return AppliedReset{}, err
	}
	if applied.Group != preview.Group || applied.Topic != preview.Topic {
		return AppliedReset{}, fmt.Errorf("%w: the Gateway answered for another group or topic", console.ErrOutcomeUnknown)
	}
	return applied, nil
}
