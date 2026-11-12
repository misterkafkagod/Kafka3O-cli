package console

import (
	"fmt"
	"path/filepath"
)

// SessionPath is <user config dir>/kafka3o/session.json; configDir is os.UserConfigDir in production.
func SessionPath(configDir func() (string, error)) (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", fmt.Errorf("find the user configuration directory: %w", err)
	}
	return filepath.Join(dir, "kafka3o", "session.json"), nil
}
