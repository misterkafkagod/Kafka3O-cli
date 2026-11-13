package groups

import (
	"fmt"
	"strconv"
	"time"
)

// ParseResetTarget reads --to: earliest, latest, an RFC 3339 time or Unix milliseconds.
func ParseResetTarget(text string) (ResetTarget, error) {
	switch text {
	case "earliest", "latest":
		return ResetTarget{Kind: text}, nil
	}
	millis, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		at, parseErr := time.Parse(time.RFC3339Nano, text)
		if parseErr != nil {
			return ResetTarget{}, fmt.Errorf("--to must be earliest, latest, an RFC 3339 time or Unix milliseconds, not %q", text)
		}
		millis = at.UnixMilli()
	}
	if millis < 0 {
		return ResetTarget{}, fmt.Errorf("--to must not be before 1970, not %q", text)
	}
	return ResetTarget{Kind: "timestamp", Timestamp: millis}, nil
}
