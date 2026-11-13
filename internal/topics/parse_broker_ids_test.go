package topics

import "testing"

func TestParseBrokerIDs(t *testing.T) {
	ids, err := ParseBrokerIDs("3, 1,2")
	if err != nil || len(ids) != 3 || ids[0] != 3 || ids[2] != 2 {
		t.Fatalf("%v %v", ids, err)
	}
	for _, bad := range []string{"", "1,,2", "a", "-1", "1,1", "1;2"} {
		if _, err := ParseBrokerIDs(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
