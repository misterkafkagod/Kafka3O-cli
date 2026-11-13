package backups

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

var partitionNumber = regexp.MustCompile(`^\d{1,9}$`)

// ParseRestoreSelection reads --from, --to and --partitions like the UI's restore form. Without any of them
// (chosen false) everything is restored and nil is returned; given but all empty, the selection is refused.
func ParseRestoreSelection(from, to, partitions string, chosen bool) (*Selection, error) {
	if !chosen {
		return nil, nil
	}
	selection := &Selection{}
	for _, bound := range []struct {
		flag, text string
		into       **time.Time
	}{{"--from", from, &selection.From}, {"--to", to, &selection.To}} {
		text := strings.TrimSpace(bound.text)
		if text == "" {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, text)
		if millis, parseErr := strconv.ParseInt(text, 10, 64); parseErr == nil {
			at, err = time.UnixMilli(millis), nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s must be an RFC 3339 time or Unix milliseconds, not %q", bound.flag, bound.text)
		}
		at = at.UTC()
		*bound.into = &at
	}
	if selection.From != nil && selection.To != nil && !selection.From.Before(*selection.To) {
		return nil, errors.New("--from must be before --to")
	}
	if strings.TrimSpace(partitions) != "" {
		for _, part := range strings.Split(partitions, ",") {
			part = strings.TrimSpace(part)
			if !partitionNumber.MatchString(part) {
				return nil, fmt.Errorf("--partitions must be numbers separated by commas, for example 0,2, not %q", partitions)
			}
			number, _ := strconv.Atoi(part)
			selection.Partitions = append(selection.Partitions, number)
		}
		slices.Sort(selection.Partitions)
		selection.Partitions = slices.Compact(selection.Partitions)
	}
	if selection.From == nil && selection.To == nil && selection.Partitions == nil {
		return nil, errors.New("choose a time range or partitions, or restore everything by leaving out --from, --to and --partitions")
	}
	return selection, nil
}
