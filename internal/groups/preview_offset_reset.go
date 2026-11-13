package groups

import (
	"context"
	"net/url"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// PreviewOffsetReset asks the Gateway what a reset would commit, without changing anything.
type PreviewOffsetReset interface {
	Execute(ctx context.Context, session console.Session, cluster, group, topic string, target ResetTarget) (ResetPreview, error)
}

type previewOffsetReset struct {
	get console.GetJSON
}

func NewPreviewOffsetReset(get console.GetJSON) PreviewOffsetReset {
	return previewOffsetReset{get: get}
}

func (p previewOffsetReset) Execute(ctx context.Context, session console.Session, cluster, group, topic string, target ResetTarget) (ResetPreview, error) {
	query := url.Values{"topic": {topic}, "target": {target.Kind}}
	if target.Kind == "timestamp" {
		query.Set("timestamp", strconv.FormatInt(target.Timestamp, 10))
	}
	var preview ResetPreview
	path := clusters.GatewayPath(cluster, "/api/v1/consumer-groups/"+url.PathEscape(group)+"/offset-reset?"+query.Encode())
	if err := p.get.Execute(ctx, session, path, &preview); err != nil {
		return ResetPreview{}, err
	}
	return preview, nil
}
