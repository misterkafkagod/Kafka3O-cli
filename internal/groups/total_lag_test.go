package groups

import "testing"

func lag(value string) *string { return &value }

func TestTotalLag(t *testing.T) {
	for _, tt := range []struct {
		partitions []Partition
		want       Lag
	}{
		{nil, Lag{Kind: "none"}},
		{[]Partition{{Lag: lag("9007199254740993")}, {Lag: lag("7")}}, Lag{Kind: "known", Lag: "9007199254741000"}},
		{[]Partition{{Lag: lag("43")}, {Lag: nil}, {Lag: nil}, {Lag: lag("not a number")}}, Lag{Kind: "unknown", Partitions: 3}},
		{[]Partition{{Lag: lag("0")}}, Lag{Kind: "known", Lag: "0"}},
	} {
		if got := TotalLag(ConsumerGroup{Partitions: tt.partitions}); got != tt.want {
			t.Errorf("%+v, want %+v", got, tt.want)
		}
	}
}
