package access

import (
	"fmt"
	"strings"
)

// ParseRole reads VIEWER, OPERATOR, ADMIN or none (no role, ""), case-insensitively.
func ParseRole(text string) (string, error) {
	switch role := strings.ToUpper(text); role {
	case "VIEWER", "OPERATOR", "ADMIN":
		return role, nil
	case "NONE":
		return "", nil
	default:
		return "", fmt.Errorf("the role must be VIEWER, OPERATOR, ADMIN or none, not %q", text)
	}
}
