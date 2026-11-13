package cli

import "time"

// describeLastWrite is when a backup last stored data, in local time.
func describeLastWrite(lastWrite *time.Time) string {
	if lastWrite == nil {
		return "nothing written yet"
	}
	return lastWrite.In(time.Local).Format("2006-01-02 15:04:05 MST")
}
