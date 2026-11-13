package audit

import (
	"strings"
	"testing"
)

func TestBuildAuditQuery(t *testing.T) {
	for _, test := range []struct {
		filters Filters
		want    string
	}{
		{Filters{}, ""},
		{Filters{Cluster: "kafka-a", Actor: "local:admin", Action: "topic.delete", Outcome: "confirmed", Limit: 5, After: "120"},
			"action=topic.delete&actor=local%3Aadmin&after=120&cluster=kafka-a&limit=5&outcome=confirmed"},
		{Filters{From: "2026-11-13T09:00:00.75-03:00", Until: "1794614400000"}, "from=2026-11-13T12%3A00%3A00Z&until=2026-11-14T00%3A00%3A00Z"},
		{Filters{From: "1763035200000", Until: "2026-11-14T00:00:00Z"}, "from=2025-11-13T12%3A00%3A00Z&until=2026-11-14T00%3A00%3A00Z"},
	} {
		if query, err := BuildAuditQuery(test.filters); err != nil || query != test.want {
			t.Errorf("%+v: %q %v", test.filters, query, err)
		}
	}
	for _, test := range []struct {
		filters Filters
		want    string
	}{
		{Filters{Cluster: "kafka a"}, `--cluster must be a cluster id such as kafka-a, not "kafka a"`},
		{Filters{Actor: strings.Repeat("a", 257)}, "--actor must be at most 256 characters"},
		{Filters{Outcome: "maybe"}, `--outcome must be one of`},
		{Filters{From: "yesterday"}, `--from must be an RFC 3339 time or Unix milliseconds, not "yesterday"`},
		{Filters{From: "2026-11-13T12:00:00Z", Until: "2026-11-13T12:00:00Z"}, "--from must be before --until"},
		{Filters{After: "-1"}, `--after must be the cursor printed with the previous page, not "-1"`},
		{Filters{Limit: 501}, "--limit must be between 1 and 500, not 501"},
		{Filters{Limit: -1}, "--limit must be between 1 and 500, not -1"},
	} {
		if _, err := BuildAuditQuery(test.filters); err == nil || !strings.HasPrefix(err.Error(), test.want) {
			t.Errorf("%+v: %v", test.filters, err)
		}
	}
}
