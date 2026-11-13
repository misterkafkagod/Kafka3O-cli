package environments

import (
	"fmt"
	"strings"
)

// ParseEnvironment reads production or non-production (or the Console's PRODUCTION / NON_PRODUCTION).
func ParseEnvironment(text string) (string, error) {
	switch strings.ToLower(text) {
	case "production":
		return "PRODUCTION", nil
	case "non-production", "non_production":
		return "NON_PRODUCTION", nil
	default:
		return "", fmt.Errorf("the environment must be production or non-production, not %q", text)
	}
}
