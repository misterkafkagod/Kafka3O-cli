package output

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// WriteNewFile creates path readable only by the user and writes data to it; an existing file is never replaced.
func WriteNewFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("%s already exists; choose another file", path)
	}
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}
