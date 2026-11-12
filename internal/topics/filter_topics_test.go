package topics

import "testing"

func TestFilterTopics(t *testing.T) {
	all := []Topic{{Name: "kafka3o-m10-verify"}, {Name: "orders"}, {Name: "__consumer_offsets", Internal: true}}
	if got := names(FilterTopics(all, "", false)); len(got) != 2 {
		t.Errorf("default: %v", got)
	}
	if got := names(FilterTopics(all, " VERIFY ", false)); len(got) != 1 || got[0] != "/kafka3o-m10-verify" {
		t.Errorf("filter: %v", got)
	}
	if got := names(FilterTopics(all, "", true)); len(got) != 3 {
		t.Errorf("internal: %v", got)
	}
	if got := FilterTopics(all, "consumer", false); got == nil || len(got) != 0 {
		t.Errorf("an internal match without --internal: %v", got)
	}
}
