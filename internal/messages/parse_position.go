package messages

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ParsePosition reads --partition, --offset and --timestamp: all empty means the latest messages; otherwise the
// partition and exactly one of offset or timestamp (RFC 3339 or Unix milliseconds), as the Gateway requires.
func ParsePosition(partition, offset, timestamp string) (*Position, error) {
	if partition == "" && offset == "" && timestamp == "" {
		return nil, nil
	}
	if partition == "" || (offset == "") == (timestamp == "") {
		return nil, errors.New("give --partition with either --offset or --timestamp")
	}
	id, err := strconv.ParseInt(partition, 10, 32)
	if err != nil || id < 0 {
		return nil, fmt.Errorf("--partition must be a partition number, not %q", partition)
	}
	position := &Position{Partition: int32(id)}
	if offset != "" {
		value, err := strconv.ParseInt(offset, 10, 64)
		if err != nil || value < 0 {
			return nil, fmt.Errorf("--offset must be a non-negative number, not %q", offset)
		}
		position.Offset = &value
		return position, nil
	}
	millis, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		at, parseErr := time.Parse(time.RFC3339Nano, timestamp)
		if parseErr != nil {
			return nil, fmt.Errorf("--timestamp must be RFC 3339 (2026-11-12T04:08:00Z) or Unix milliseconds, not %q", timestamp)
		}
		millis = at.UnixMilli()
	}
	if millis < 0 {
		return nil, fmt.Errorf("--timestamp must not be before 1970, not %q", timestamp)
	}
	position.Timestamp = &millis
	return position, nil
}
