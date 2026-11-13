package messages

import "testing"

func TestParsePosition(t *testing.T) {
	if position, err := ParsePosition("", "", ""); position != nil || err != nil {
		t.Fatalf("latest: %+v %v", position, err)
	}
	position, err := ParsePosition("0", "61", "")
	if err != nil || position.Partition != 0 || *position.Offset != 61 || position.Timestamp != nil {
		t.Fatalf("offset: %+v %v", position, err)
	}
	for _, timestamp := range []string{"2026-11-12T04:08:00Z", "1794456480000"} {
		position, err = ParsePosition("2", "", timestamp)
		if err != nil || position.Partition != 2 || *position.Timestamp != 1794456480000 || position.Offset != nil {
			t.Fatalf("%s: %+v %v", timestamp, position, err)
		}
	}
	for _, bad := range [][3]string{{"", "5", ""}, {"", "", "1"}, {"0", "", ""}, {"0", "1", "2"}, {"x", "1", ""}, {"-1", "1", ""},
		{"0", "-5", ""}, {"0", "abc", ""}, {"0", "", "yesterday"}, {"0", "", "-10"}} {
		if _, err := ParsePosition(bad[0], bad[1], bad[2]); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}
