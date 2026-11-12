package topics

import "testing"

func TestGroupsReadingTopic(t *testing.T) {
	lag := "3"
	groups := []ConsumerGroup{
		{ID: "reader", State: "Stable", Topics: []string{"orders", "payments"}, Partitions: []GroupPartition{
			{Topic: "orders", Partition: 0, Lag: &lag}, {Topic: "payments", Partition: 0}}},
		{ID: "other", State: "Empty", Topics: []string{"payments"}},
		{ID: "broken", State: "Dead", Error: "COORDINATOR_NOT_AVAILABLE"},
	}
	got := GroupsReadingTopic(groups, "orders")
	if len(got.Groups) != 1 || got.Groups[0].ID != "reader" || len(got.Groups[0].Partitions) != 1 || got.Groups[0].Partitions[0].Topic != "orders" || got.Unresolved != 1 {
		t.Fatalf("%+v", got)
	}
	if len(groups[0].Partitions) != 2 {
		t.Fatal("the input was changed")
	}
	if none := GroupsReadingTopic(nil, "orders"); none.Groups == nil || len(none.Groups) != 0 || none.Unresolved != 0 {
		t.Fatalf("no groups: %+v", none)
	}
}
