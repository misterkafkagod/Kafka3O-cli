package topics

import "errors"

// ValidateNewTopic refuses a request the Gateway would refuse, before anything is sent.
func ValidateNewTopic(topic NewTopic) error {
	if err := ValidateTopicName(topic.Name); err != nil {
		return err
	}
	if topic.Partitions < 1 || topic.ReplicationFactor < 1 {
		return errors.New("--partitions and --replication-factor must be at least 1")
	}
	return nil
}
