package groups

import (
	"fmt"
	"io"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// ErrGroupNotFound means the cluster answered but has no group with that id.
type ErrGroupNotFound struct {
	Cluster, Group string
}

func (e ErrGroupNotFound) Error() string {
	return "group " + e.Group + " was not found on " + e.Cluster
}

// WriteGroup writes the group page: state, members, and per topic each partition's offsets, lag and status.
// Offsets Kafka did not report read "unknown", never 0.
func WriteGroup(w io.Writer, group ConsumerGroup) error {
	fmt.Fprintf(w, "Group %s on %s\nState: %s (%s protocol)\n", group.ID, group.Cluster, group.State, group.Protocol)
	if group.Error != "" {
		fmt.Fprintf(w, "Unavailable: %s\n", group.Error)
	}
	fmt.Fprint(w, "\nMembers\n")
	if len(group.Members) == 0 {
		fmt.Fprintln(w, "No members.")
	} else {
		rows := [][]string{}
		for _, member := range group.Members {
			instance := ""
			if member.InstanceID != nil {
				instance = *member.InstanceID
			}
			rows = append(rows, []string{member.ID, member.ClientID, member.ClientHost, instance})
		}
		if err := output.WriteTable(w, []string{"MEMBER", "CLIENT", "HOST", "INSTANCE"}, rows); err != nil {
			return err
		}
	}
	byTopic := map[string][]Partition{}
	topics := []string{}
	for _, partition := range group.Partitions {
		if _, seen := byTopic[partition.Topic]; !seen {
			topics = append(topics, partition.Topic)
		}
		byTopic[partition.Topic] = append(byTopic[partition.Topic], partition)
	}
	if len(topics) == 0 {
		_, err := fmt.Fprint(w, "\nNo committed offsets.\n")
		return err
	}
	for _, topic := range topics {
		fmt.Fprintf(w, "\nOffsets for %s\n", topic)
		rows := [][]string{}
		for _, partition := range byTopic[topic] {
			status := StatusLabel(partition.Status)
			if partition.Error != "" {
				status += ": " + partition.Error
			}
			rows = append(rows, []string{strconv.Itoa(int(partition.Partition)), offsetText(partition.CommittedOffset), offsetText(partition.StartOffset),
				offsetText(partition.EndOffset), offsetText(partition.Lag), status})
		}
		if err := output.WriteTable(w, []string{"PARTITION", "COMMITTED", "START", "END", "LAG", "STATUS"}, rows); err != nil {
			return err
		}
	}
	return nil
}
