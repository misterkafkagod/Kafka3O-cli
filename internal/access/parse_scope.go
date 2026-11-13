package access

import (
	"fmt"
	"regexp"
)

var clusterID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// ParseScope reads `all` (the global scope, "") or a cluster id.
func ParseScope(text string) (string, error) {
	if text == "all" {
		return "", nil
	}
	if !clusterID.MatchString(text) {
		return "", fmt.Errorf("the scope must be all or a cluster id such as kafka-a, not %q", text)
	}
	return text, nil
}
