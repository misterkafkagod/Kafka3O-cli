package topics

import "testing"

func TestValidateNewTopic(t *testing.T) {
	if err := ValidateNewTopic(NewTopic{Name: "t", Partitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []NewTopic{{Name: "bad name", Partitions: 1, ReplicationFactor: 1}, {Name: "t", Partitions: 0, ReplicationFactor: 1}, {Name: "t", Partitions: 1}} {
		if err := ValidateNewTopic(bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}
