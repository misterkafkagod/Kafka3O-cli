package audit

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"time"
)

var (
	clusterID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	outcomes  = []string{"attempted", "denied", "validation_rejected", "accepted", "confirmed", "failed_before_send", "unknown", "succeeded", "failed"}
)

// BuildAuditQuery checks the filters like the Console's audit query and encodes them; times are sent as UTC
// RFC 3339. A zero Limit leaves the Console's default.
func BuildAuditQuery(filters Filters) (string, error) {
	query := url.Values{}
	if filters.Cluster != "" {
		if !clusterID.MatchString(filters.Cluster) {
			return "", fmt.Errorf("--cluster must be a cluster id such as kafka-a, not %q", filters.Cluster)
		}
		query.Set("cluster", filters.Cluster)
	}
	for _, text := range []struct{ flag, name, value string }{{"--actor", "actor", filters.Actor}, {"--action", "action", filters.Action}} {
		if len(text.value) > 256 {
			return "", fmt.Errorf("%s must be at most 256 characters", text.flag)
		}
		if text.value != "" {
			query.Set(text.name, text.value)
		}
	}
	if filters.Outcome != "" && !slices.Contains(outcomes, filters.Outcome) {
		return "", fmt.Errorf("--outcome must be one of %v, not %q", outcomes, filters.Outcome)
	}
	if filters.Outcome != "" {
		query.Set("outcome", filters.Outcome)
	}
	var bounds [2]time.Time
	for i, bound := range []struct{ flag, name, value string }{{"--from", "from", filters.From}, {"--until", "until", filters.Until}} {
		if bound.value == "" {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, bound.value)
		if millis, parseErr := strconv.ParseInt(bound.value, 10, 64); parseErr == nil {
			at, err = time.UnixMilli(millis), nil
		}
		if err != nil {
			return "", fmt.Errorf("%s must be an RFC 3339 time or Unix milliseconds, not %q", bound.flag, bound.value)
		}
		bounds[i] = at.UTC().Truncate(time.Second)
		query.Set(bound.name, bounds[i].Format(time.RFC3339))
	}
	if !bounds[0].IsZero() && !bounds[1].IsZero() && !bounds[0].Before(bounds[1]) {
		return "", errors.New("--from must be before --until")
	}
	if filters.After != "" {
		if after, err := strconv.ParseInt(filters.After, 10, 64); err != nil || after < 0 {
			return "", fmt.Errorf("--after must be the cursor printed with the previous page, not %q", filters.After)
		}
		query.Set("after", filters.After)
	}
	if filters.Limit < 0 || filters.Limit > MaxLimit {
		return "", fmt.Errorf("--limit must be between 1 and %d, not %d", MaxLimit, filters.Limit)
	}
	if filters.Limit > 0 {
		query.Set("limit", strconv.Itoa(filters.Limit))
	}
	return query.Encode(), nil
}
