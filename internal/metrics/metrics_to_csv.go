package metrics

import (
	"encoding/json"
	"math/big"
	"strings"
)

// MetricsToCSV is a port of the UI's metricsToCsv: one row per value with the header
// metric,broker,logDir,topic,name,value; a value Kafka did not report, or a section it could not answer, is empty.
func MetricsToCSV(m ClusterMetrics) string {
	rows := [][6]string{{"read_at", "", "", "", "", m.ReadAt}}
	for _, dir := range m.LogDirs.Dirs {
		used := ""
		if dir.Error == "" && dir.TotalBytes != nil && dir.UsableBytes != nil {
			total, okTotal := new(big.Int).SetString(dir.TotalBytes.String(), 10)
			usable, okUsable := new(big.Int).SetString(dir.UsableBytes.String(), 10)
			if okTotal && okUsable {
				used = new(big.Int).Sub(total, usable).String()
			}
		}
		values := [4]string{numberText(dir.TotalBytes), used, numberText(dir.UsableBytes), numberText(dir.KafkaBytes)}
		for i, metric := range [4]string{"log_dir_total_bytes", "log_dir_used_bytes", "log_dir_usable_bytes", "log_dir_kafka_bytes"} {
			value := values[i]
			if dir.Error != "" {
				value = ""
			}
			rows = append(rows, [6]string{metric, dir.Broker.String(), dir.Dir, "", "", value})
		}
	}
	p := m.Partitions
	for _, count := range []struct {
		metric string
		value  *json.Number
	}{{"topics", p.Topics}, {"partitions", p.Partitions}, {"partitions_under_replicated", p.UnderReplicated},
		{"partitions_below_min_isr", p.BelowMinISR}, {"partitions_offline", p.Offline}} {
		value := numberText(count.value)
		if p.Error != "" {
			value = ""
		}
		rows = append(rows, [6]string{count.metric, "", "", "", "", value})
	}
	for _, topic := range m.LargestTopics.Topics {
		rows = append(rows, [6]string{"topic_size_bytes", "", "", topic.Topic, "", topic.Bytes.String()})
	}
	for _, topic := range m.EndOffsets.Topics {
		value := ""
		if topic.EndOffset != nil {
			value = *topic.EndOffset
		}
		rows = append(rows, [6]string{"topic_end_offset", "", "", topic.Topic, "", value})
	}
	if q := m.Quorum; q != nil {
		leader, watermark := numberText(q.LeaderID), q.HighWatermark.String()
		if q.Error != "" {
			leader, watermark = "", ""
		}
		rows = append(rows, [6]string{"quorum_leader", "", "", "", "", leader}, [6]string{"quorum_high_watermark", "", "", "", "", watermark})
		if q.Error == "" {
			for _, replica := range q.Replicas {
				rows = append(rows, [6]string{"quorum_replica_lag", replica.ID.String(), "", "", replica.Role, numberText(replica.Lag)})
			}
		}
	}
	if b := m.Brokers; b != nil && b.Error == "" {
		for _, broker := range b.Brokers {
			id := broker.Broker.String()
			rows = append(rows, [6]string{"broker_leaders", id, "", "", "", broker.Leaders.String()},
				[6]string{"broker_replicas", id, "", "", "", broker.Replicas.String()},
				[6]string{"broker_non_preferred_leaders", id, "", "", "", broker.NonPreferredLeaders.String()})
		}
	}
	if g := m.ConsumerGroups; g != nil && g.Error == "" {
		for _, state := range g.States {
			rows = append(rows, [6]string{"consumer_groups", "", "", "", state.State, state.Count.String()})
		}
		for _, group := range g.Groups {
			// A partly known lag is a lower bound, so it is left empty like any value Kafka did not fully report.
			lag, largest := "", ""
			if group.UnknownPartitions.String() == "0" && group.Error == "" {
				if group.Lag != nil {
					lag = *group.Lag
				}
				if group.MaxPartitionLag != nil {
					largest = *group.MaxPartitionLag
				}
			}
			rows = append(rows, [6]string{"consumer_group_members", "", "", "", group.Group, group.Members.String()},
				[6]string{"consumer_group_lag", "", "", "", group.Group, lag},
				[6]string{"consumer_group_max_partition_lag", "", "", "", group.Group, largest},
				[6]string{"consumer_group_unknown_lag_partitions", "", "", "", group.Group, group.UnknownPartitions.String()})
		}
	}
	if c := m.Connect; c != nil && !c.NotConfigured && c.Error == "" {
		for _, state := range c.Connectors {
			rows = append(rows, [6]string{"connect_connectors", "", "", "", state.State, state.Count.String()})
		}
		for _, state := range c.Tasks {
			rows = append(rows, [6]string{"connect_tasks", "", "", "", state.State, state.Count.String()})
		}
	}
	if b := m.Backups; b != nil && !b.NotConfigured && b.Error == "" {
		for _, backup := range b.Backups {
			up, restoring, lastWrite := "0", "0", ""
			if backup.State == "RUNNING" {
				up = "1"
			}
			if backup.Restoring {
				restoring = "1"
			}
			if backup.LastWrite != nil {
				lastWrite = *backup.LastWrite
			}
			rows = append(rows, [6]string{"backup_up", "", "", backup.Topic, backup.State, up},
				[6]string{"backup_restoring", "", "", backup.Topic, "", restoring},
				[6]string{"backup_stored_bytes", "", "", backup.Topic, "", numberText(backup.StoredBytes)},
				[6]string{"backup_last_write", "", "", backup.Topic, "", lastWrite})
		}
	}
	var out strings.Builder
	out.WriteString("metric,broker,logDir,topic,name,value\r\n")
	for _, row := range rows {
		for i, field := range row {
			if i > 0 {
				out.WriteByte(',')
			}
			out.WriteString(EscapeCSVField(field))
		}
		out.WriteString("\r\n")
	}
	return out.String()
}
