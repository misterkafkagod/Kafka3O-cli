package cli

import (
	"fmt"
	"io"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// WriteTopicView writes the topic's sections as the UI's topic page shows them; a section that could not be read
// says why. Offsets and lag are printed exactly as the Gateway sent them.
func WriteTopicView(w io.Writer, view topics.TopicView) error {
	fmt.Fprintf(w, "Topic %s on %s\n\nPartitions\n", view.Name, view.Cluster)
	partitions := make([][]string, 0, len(view.Partitions))
	for _, p := range view.Partitions {
		partitions = append(partitions, []string{strconv.Itoa(int(p.ID)), strconv.Itoa(int(p.Leader)), joinIDs(p.Replicas), joinIDs(p.ISR),
			strconv.FormatInt(p.StartOffset, 10), strconv.FormatInt(p.EndOffset, 10)})
	}
	if err := output.WriteTable(w, []string{"PARTITION", "LEADER", "REPLICAS", "ISR", "START", "END"}, partitions); err != nil {
		return err
	}

	fmt.Fprint(w, "\nConfiguration\n")
	config := make([][]string, 0, len(view.Config))
	for _, entry := range view.Config {
		value := entry.Value
		if entry.Sensitive {
			value = "(sensitive, hidden)"
		}
		config = append(config, []string{entry.Name, value})
	}
	if err := output.WriteTable(w, []string{"NAME", "VALUE"}, config); err != nil {
		return err
	}

	fmt.Fprint(w, "\nConsumer groups\n")
	switch {
	case view.Groups.Error != "":
		fmt.Fprintf(w, "Unavailable: %s\n", view.Groups.Error)
	case len(view.Groups.Groups) == 0:
		fmt.Fprintln(w, "No consumer groups read this topic.")
	default:
		rows := [][]string{}
		for _, group := range view.Groups.Groups {
			for _, partition := range group.Partitions {
				rows = append(rows, []string{group.ID, group.State, strconv.Itoa(int(partition.Partition)), orUnknown(partition.CommittedOffset),
					orUnknown(partition.EndOffset), orUnknown(partition.Lag)})
			}
			if len(group.Partitions) == 0 {
				rows = append(rows, []string{group.ID, group.State, "", "", "", ""})
			}
		}
		if err := output.WriteTable(w, []string{"GROUP", "STATE", "PARTITION", "COMMITTED", "END", "LAG"}, rows); err != nil {
			return err
		}
	}
	if view.Groups.Unresolved > 0 {
		fmt.Fprintf(w, "%d consumer group(s) could not be described and may also read this topic.\n", view.Groups.Unresolved)
	}

	fmt.Fprint(w, "\nBackup\n")
	backup := view.Backup.Backup
	switch {
	case view.Backup.NotConfigured:
		fmt.Fprintln(w, "Backups are not configured for this cluster.")
	case view.Backup.Error != "":
		fmt.Fprintf(w, "Unavailable: %s\n", view.Backup.Error)
	case backup == nil:
		fmt.Fprintln(w, "No backup for this topic.")
	default:
		state := backup.State
		if backup.Error != "" {
			state += ": " + backup.Error
		}
		lines := [][]string{{"State", state}, {"Location", backup.Location}}
		if backup.Storage != nil {
			lines = append(lines, []string{"Last write", describeLastWrite(backup.Storage.LastWrite)},
				[]string{"Stored", fmt.Sprintf("%d bytes in %d objects", backup.Storage.StoredBytes, backup.Storage.Objects)})
		}
		if settings := describeBackupSettings(backup.Settings); settings != "" {
			lines = append(lines, []string{"Settings", settings})
		}
		if restore := describeBackupRestore(backup.Restore); restore != "" {
			lines = append(lines, []string{"Restore", restore})
		}
		for _, line := range lines {
			fmt.Fprintf(w, "%-11s %s\n", line[0]+":", line[1])
		}
	}
	return nil
}
