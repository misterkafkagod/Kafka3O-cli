package topics

// GroupsReadingTopic keeps the groups whose topics include the topic, with only that topic's partitions, and counts
// the groups the Gateway could not describe (the UI's rule).
func GroupsReadingTopic(groups []ConsumerGroup, topic string) TopicGroups {
	result := TopicGroups{Groups: []ConsumerGroup{}}
	for _, group := range groups {
		reads := false
		for _, name := range group.Topics {
			reads = reads || name == topic
		}
		switch {
		case reads:
			partitions := []GroupPartition{}
			for _, partition := range group.Partitions {
				if partition.Topic == topic {
					partitions = append(partitions, partition)
				}
			}
			group.Partitions = partitions
			result.Groups = append(result.Groups, group)
		case group.Error != "":
			result.Unresolved++
		}
	}
	return result
}
