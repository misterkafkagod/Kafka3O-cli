package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/misterkafkagod/Kafka3O-cli/internal/accounts"
)

// readNewPassword reads a temporary password from the first line of standard input (fromStdin), or hidden twice
// from the terminal, and checks the Console's length policy. It is never printed.
func readNewPassword(input *bufio.Reader, fromStdin bool, read ReadPassword) (string, error) {
	var password string
	if fromStdin {
		line, err := input.ReadString('\n')
		if err != nil && (err != io.EOF || line == "") {
			return "", errors.New("--password-stdin: no password on standard input")
		}
		password = strings.TrimRight(line, "\r\n")
	} else {
		first, err := read.Execute("Temporary password: ")
		if err != nil {
			return "", err
		}
		again, err := read.Execute("Temporary password again: ")
		if err != nil {
			return "", err
		}
		if first != again {
			return "", errors.New("the two passwords do not match")
		}
		password = first
	}
	if err := accounts.ValidateTemporaryPassword(password); err != nil {
		return "", fmt.Errorf("%w; nothing was sent", err)
	}
	return password, nil
}
