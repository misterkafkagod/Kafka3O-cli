package topics

import (
	"context"
	"errors"
	"net/url"
	"sync"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// DescribeTopic reads one topic's partitions, configuration, consumer groups and backup.
type DescribeTopic interface {
	Execute(ctx context.Context, session console.Session, cluster, topic string) (TopicView, error)
}

type describeTopic struct {
	accessible clusters.ListAccessibleClusters
	get        console.GetJSON
}

func NewDescribeTopic(accessible clusters.ListAccessibleClusters, get console.GetJSON) DescribeTopic {
	return describeTopic{accessible: accessible, get: get}
}

// Execute reads the three Gateway endpoints concurrently. The topic itself must answer; the groups and the backup
// keep their own errors so a failing part does not hide the rest.
func (d describeTopic) Execute(ctx context.Context, session console.Session, cluster, topic string) (TopicView, error) {
	accessible, err := d.accessible.Execute(ctx, session)
	if err != nil {
		return TopicView{}, err
	}
	known := false
	for _, candidate := range accessible {
		known = known || candidate.ID == cluster
	}
	if !known {
		return TopicView{}, clusters.ErrUnknownCluster{Cluster: cluster}
	}
	var detail struct {
		Partitions []TopicPartition `json:"partitions"`
		Config     []TopicConfig    `json:"config"`
	}
	var groups []ConsumerGroup
	var backups struct {
		Backups []struct {
			Topic string `json:"topic"`
			BackupSnapshot
		} `json:"backups"`
	}
	var detailErr, groupsErr, backupsErr error
	var wait sync.WaitGroup
	wait.Go(func() {
		detailErr = d.get.Execute(ctx, session, clusters.GatewayPath(cluster, "/api/v1/topics/"+url.PathEscape(topic)), &detail)
	})
	wait.Go(func() {
		groupsErr = d.get.Execute(ctx, session, clusters.GatewayPath(cluster, "/api/v1/consumer-groups"), &groups)
	})
	wait.Go(func() {
		backupsErr = d.get.Execute(ctx, session, clusters.GatewayPath(cluster, "/api/v1/backups"), &backups)
	})
	wait.Wait()

	for _, failure := range []error{detailErr, groupsErr, backupsErr} {
		if errors.Is(failure, console.ErrSignedOut) {
			return TopicView{}, failure
		}
	}
	var problem console.Problem
	if errors.As(detailErr, &problem) && problem.Status == 404 && problem.Code == "not_found" {
		return TopicView{}, ErrTopicNotFound{Cluster: cluster, Topic: topic}
	}
	if detailErr != nil {
		status, reason := clusters.ClassifyClusterError(detailErr)
		return TopicView{}, ErrClusterRead{Cluster: cluster, Status: status, Reason: reason}
	}
	view := TopicView{Cluster: cluster, Name: topic, Partitions: detail.Partitions, Config: detail.Config}
	if view.Partitions == nil {
		view.Partitions = []TopicPartition{}
	}
	if view.Config == nil {
		view.Config = []TopicConfig{}
	}
	if groupsErr != nil {
		view.Groups = TopicGroups{Error: SectionReason(groupsErr), Groups: []ConsumerGroup{}}
	} else {
		view.Groups = GroupsReadingTopic(groups, topic)
	}
	switch {
	case errors.As(backupsErr, &problem) && problem.Code == "backups_not_configured":
		view.Backup = TopicBackup{NotConfigured: true}
	case backupsErr != nil:
		view.Backup = TopicBackup{Error: SectionReason(backupsErr)}
	default:
		for _, backup := range backups.Backups {
			if backup.Topic == topic {
				snapshot := backup.BackupSnapshot
				view.Backup.Backup = &snapshot
			}
		}
	}
	return view, nil
}
