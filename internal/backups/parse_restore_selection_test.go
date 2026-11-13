package backups

import (
	"encoding/json"
	"testing"
)

func TestParseRestoreSelection(t *testing.T) {
	if selection, err := ParseRestoreSelection("", "", "", false); selection != nil || err != nil {
		t.Fatalf("everything: %+v %v", selection, err)
	}
	for _, test := range []struct{ from, to, partitions, want string }{
		{"2026-11-13T09:00:00-03:00", "", "", `{"from":"2026-11-13T12:00:00Z"}`},
		{"", "1763035200000", "", `{"to":"2025-11-13T12:00:00Z"}`},
		{"", "", " 2, 0,2 ", `{"partitions":[0,2]}`},
		{"1763035200000", "2026-11-13T12:00:00.5Z", "1", `{"from":"2025-11-13T12:00:00Z","to":"2026-11-13T12:00:00.5Z","partitions":[1]}`},
	} {
		selection, err := ParseRestoreSelection(test.from, test.to, test.partitions, true)
		encoded, _ := json.Marshal(selection)
		if err != nil || string(encoded) != test.want {
			t.Errorf("%+v: %s %v", test, encoded, err)
		}
	}
	for _, test := range []struct{ from, to, partitions, want string }{
		{"yesterday", "", "", `--from must be an RFC 3339 time or Unix milliseconds, not "yesterday"`},
		{"", "2026-13-01", "", `--to must be an RFC 3339 time or Unix milliseconds, not "2026-13-01"`},
		{"2026-11-13T12:00:00Z", "2026-11-13T11:00:00Z", "", "--from must be before --to"},
		{"2026-11-13T12:00:00Z", "2026-11-13T12:00:00Z", "", "--from must be before --to"},
		{"", "", "0,x", `--partitions must be numbers separated by commas, for example 0,2, not "0,x"`},
		{"", "", "-1", `--partitions must be numbers separated by commas, for example 0,2, not "-1"`},
		{"", "", "0,,1", `--partitions must be numbers separated by commas, for example 0,2, not "0,,1"`},
		{" ", "", "", "choose a time range or partitions, or restore everything by leaving out --from, --to and --partitions"},
	} {
		if _, err := ParseRestoreSelection(test.from, test.to, test.partitions, true); err == nil || err.Error() != test.want {
			t.Errorf("%+v: %v", test, err)
		}
	}
}
