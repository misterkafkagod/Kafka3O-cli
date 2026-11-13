package cli

import (
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// topicReadError turns a failed read of one topic into the command's error: signed out stays (exit 3), a missing
// topic is "not found" (exit 1), anything else is the cluster failing (exit 2).
func topicReadError(cluster, topic string, err error) error {
	if err == nil || errors.Is(err, console.ErrSignedOut) {
		return err
	}
	var problem console.Problem
	if errors.As(err, &problem) && problem.Status == 404 && problem.Code == "not_found" {
		return topics.ErrTopicNotFound{Cluster: cluster, Topic: topic}
	}
	status, reason := clusters.ClassifyClusterError(err)
	return fmt.Errorf("%s: %s: %w", cluster, describeClusterStatus(clusters.ClusterHealth{Status: status, Error: reason}), ErrClusterFailed)
}
