package console

import (
	"errors"
	"io/fs"
	"os"
)

// DeleteSession removes the session file; a file that is already gone is not an error.
func DeleteSession(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
