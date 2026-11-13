package metrics

import (
	"fmt"
	"io"
	"strings"

	"github.com/misterkafkagod/Kafka3O-cli/internal/output"
)

// WriteMetricsText writes the Metrics tab's sections for a terminal; a section Kafka could not answer says why,
// and a value Kafka did not report reads "unknown", never 0.
func WriteMetricsText(w io.Writer, cluster string, m ClusterMetrics) error {
	fmt.Fprintf(w, "Metrics for %s, read at %s\n\nPartition health\n", cluster, m.ReadAt)
	if p := m.Partitions; p.Error != "" {
		fmt.Fprintf(w, "Unavailable: %s\n", p.Error)
	} else {
		fmt.Fprintf(w, "Topics %s, partitions %s, under-replicated %s, below min ISR %s, offline %s\n", unknownIfEmpty(numberText(p.Topics)),
			unknownIfEmpty(numberText(p.Partitions)), unknownIfEmpty(numberText(p.UnderReplicated)), unknownIfEmpty(numberText(p.BelowMinISR)), unknownIfEmpty(numberText(p.Offline)))
		if p.BelowMinISR == nil && p.MinISRError != "" {
			fmt.Fprintf(w, "Below min ISR is unknown: %s\n", p.MinISRError)
		}
		if len(p.UnreadableTopics) > 0 {
			fmt.Fprintf(w, "Not counted, because Kafka did not describe them: %s\n", strings.Join(p.UnreadableTopics, ", "))
		}
	}

	if q := m.Quorum; q != nil {
		fmt.Fprint(w, "\nController quorum\n")
		if q.Error != "" {
			fmt.Fprintf(w, "Unavailable: %s\n", q.Error)
		} else {
			leader := numberText(q.LeaderID)
			if leader == "" {
				leader = "none"
			}
			fmt.Fprintf(w, "Leader %s, epoch %s, high watermark %s\n", leader, q.LeaderEpoch, q.HighWatermark)
			rows := [][]string{}
			for _, replica := range q.Replicas {
				fetch := "unknown"
				if replica.LastFetch != nil {
					fetch = *replica.LastFetch
				}
				rows = append(rows, []string{replica.ID.String(), replica.Role, replica.LogEndOffset.String(), unknownIfEmpty(numberText(replica.Lag)), fetch})
			}
			if err := output.WriteTable(w, []string{"REPLICA", "ROLE", "LOG END", "LAG", "LAST FETCH"}, rows); err != nil {
				return err
			}
		}
	}

	fmt.Fprint(w, "\nBroker disk use (the volume holding each log directory, which other data may share)\n")
	if m.LogDirs.Error != "" && len(m.LogDirs.Dirs) == 0 {
		fmt.Fprintf(w, "Unavailable: %s\n", m.LogDirs.Error)
	} else {
		rows := [][]string{}
		for _, dir := range m.LogDirs.Dirs {
			if dir.Error != "" {
				rows = append(rows, []string{dir.Broker.String(), dir.Dir, "Unavailable: " + dir.Error, "", ""})
				continue
			}
			rows = append(rows, []string{dir.Broker.String(), dir.Dir, unknownIfEmpty(numberText(dir.TotalBytes)), unknownIfEmpty(numberText(dir.UsableBytes)), unknownIfEmpty(numberText(dir.KafkaBytes))})
		}
		if err := output.WriteTable(w, []string{"BROKER", "LOG DIRECTORY", "TOTAL BYTES", "FREE BYTES", "KAFKA BYTES"}, rows); err != nil {
			return err
		}
	}

	if b := m.Brokers; b != nil {
		fmt.Fprint(w, "\nPartition leaders per broker\n")
		if b.Error != "" {
			fmt.Fprintf(w, "Unavailable: %s\n", b.Error)
		} else {
			rows := [][]string{}
			for _, broker := range b.Brokers {
				rows = append(rows, []string{broker.Broker.String(), broker.Leaders.String(), broker.Replicas.String(), broker.NonPreferredLeaders.String()})
			}
			if err := output.WriteTable(w, []string{"BROKER", "LEADERS", "REPLICAS", "NOT PREFERRED LEADER"}, rows); err != nil {
				return err
			}
		}
	}

	if g := m.ConsumerGroups; g != nil {
		fmt.Fprint(w, "\nConsumer groups\n")
		switch {
		case g.Error != "":
			fmt.Fprintf(w, "Unavailable: %s\n", g.Error)
		case len(g.Groups) == 0:
			fmt.Fprintln(w, "No consumer groups.")
		default:
			states := []string{}
			for _, state := range g.States {
				states = append(states, state.State+" "+state.Count.String())
			}
			fmt.Fprintln(w, strings.Join(states, ", "))
			rows := [][]string{}
			for _, group := range g.Groups {
				lag, largest := "", ""
				if group.MaxPartitionLag != nil {
					largest = *group.MaxPartitionLag
				}
				switch unknown := group.UnknownPartitions.String(); {
				case group.Error != "":
					lag, largest = "Unavailable: "+group.Error, ""
				case group.Lag == nil && unknown != "0":
					lag = "unknown (" + unknown + " partitions)"
				case group.Lag == nil:
					lag = "no partitions"
				case unknown != "0":
					lag = "at least " + *group.Lag + " (" + unknown + " partitions unknown)"
				default:
					lag = *group.Lag
				}
				rows = append(rows, []string{group.Group, group.State, group.Members.String(), lag, largest})
			}
			if err := output.WriteTable(w, []string{"GROUP", "STATE", "MEMBERS", "TOTAL LAG", "LARGEST PARTITION LAG"}, rows); err != nil {
				return err
			}
		}
	}

	if c := m.Connect; c != nil {
		fmt.Fprint(w, "\nKafka Connect\n")
		switch {
		case c.NotConfigured:
			fmt.Fprintln(w, "Kafka Connect is not configured for this cluster.")
		case c.Error != "":
			fmt.Fprintf(w, "Unavailable: %s\n", c.Error)
		default:
			counts := []string{}
			for _, state := range c.Connectors {
				counts = append(counts, "connectors "+strings.ToLower(state.State)+" "+state.Count.String())
			}
			for _, state := range c.Tasks {
				counts = append(counts, "tasks "+strings.ToLower(state.State)+" "+state.Count.String())
			}
			fmt.Fprintln(w, strings.Join(counts, ", "))
			for _, failure := range c.Failures {
				where := failure.Connector
				if failure.Task != nil {
					where += " task " + failure.Task.String()
				}
				fmt.Fprintf(w, "Failed: %s: %s\n", where, failure.Error)
			}
		}
	}

	if b := m.Backups; b != nil {
		fmt.Fprint(w, "\nBackups\n")
		switch {
		case b.NotConfigured:
			fmt.Fprintln(w, "Backups are not configured for this cluster.")
		case b.Error != "":
			fmt.Fprintf(w, "Unavailable: %s\n", b.Error)
		case len(b.Backups) == 0:
			fmt.Fprintln(w, "No backups yet.")
		default:
			rows := [][]string{}
			for _, backup := range b.Backups {
				state, last, restore := backup.State, "unknown", ""
				if backup.Error != "" {
					state += ": " + backup.Error
				}
				if backup.StorageListed {
					last = "nothing written yet"
				}
				if backup.LastWrite != nil {
					last = *backup.LastWrite
				}
				if backup.Restoring {
					restore = "running"
				}
				rows = append(rows, []string{backup.Topic, state, last, unknownIfEmpty(numberText(backup.StoredBytes)), restore})
			}
			if err := output.WriteTable(w, []string{"TOPIC", "STATE", "LAST WRITE", "STORED BYTES", "RESTORE"}, rows); err != nil {
				return err
			}
		}
	}

	fmt.Fprint(w, "\nLargest topics (size on disk, all replicas)\n")
	if m.LargestTopics.Error != "" {
		fmt.Fprintf(w, "Unavailable: %s\n", m.LargestTopics.Error)
		return nil
	}
	rows := [][]string{}
	for _, topic := range m.LargestTopics.Topics {
		rows = append(rows, []string{topic.Topic, topic.Bytes.String()})
	}
	return output.WriteTable(w, []string{"TOPIC", "BYTES"}, rows)
}
