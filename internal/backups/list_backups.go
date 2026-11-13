package backups

import (
	"context"
	"errors"
	"slices"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ListBackups reads the topic backups of one cluster or of every accessible cluster.
type ListBackups interface {
	Execute(ctx context.Context, session console.Session, cluster string) ([]ClusterBackups, []clusters.ClusterFailure, error)
}

type listBackups struct {
	accessible clusters.ListAccessibleClusters
	get        console.GetJSON
}

func NewListBackups(accessible clusters.ListAccessibleClusters, get console.GetJSON) ListBackups {
	return listBackups{accessible: accessible, get: get}
}

// Execute keeps the Console's cluster order; a cluster without backup storage is NotConfigured, not a failure.
func (l listBackups) Execute(ctx context.Context, session console.Session, cluster string) ([]ClusterBackups, []clusters.ClusterFailure, error) {
	return clusters.ReadAcrossClusters(ctx, session, l.accessible, cluster,
		func(ctx context.Context, candidate clusters.AccessibleCluster) (ClusterBackups, error) {
			var answer struct {
				Backups []Backup `json:"backups"`
			}
			err := l.get.Execute(ctx, session, clusters.GatewayPath(candidate.ID, "/api/v1/backups"), &answer)
			var problem console.Problem
			if errors.As(err, &problem) && problem.Code == "backups_not_configured" {
				return ClusterBackups{Cluster: candidate.ID, NotConfigured: true, Backups: []Backup{}}, nil
			}
			if err != nil {
				return ClusterBackups{}, err
			}
			result := ClusterBackups{Cluster: candidate.ID, Backups: []Backup{}}
			for _, backup := range answer.Backups {
				backup.Cluster = candidate.ID
				result.Backups = append(result.Backups, backup)
			}
			slices.SortFunc(result.Backups, CompareBackups)
			return result, nil
		})
}
