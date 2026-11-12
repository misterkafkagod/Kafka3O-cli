// Package terminal reads secrets from the user's terminal without echoing them.
package terminal

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"
)

// ErrNoTerminal means a password cannot be asked for interactively; --password-stdin must be used instead.
var ErrNoTerminal = errors.New("standard input is not a terminal; use --password-stdin")

type ReadHiddenPassword struct {
	input  *os.File
	prompt io.Writer
}

// NewReadHiddenPassword reads from input (normally os.Stdin) and writes the prompt to prompt (normally os.Stderr),
// so a password never ends up in a command's output.
func NewReadHiddenPassword(input *os.File, prompt io.Writer) ReadHiddenPassword {
	return ReadHiddenPassword{input: input, prompt: prompt}
}

func (r ReadHiddenPassword) Execute(prompt string) (string, error) {
	fd := int(r.input.Fd())
	if !term.IsTerminal(fd) {
		return "", ErrNoTerminal
	}
	fmt.Fprint(r.prompt, prompt)
	password, err := term.ReadPassword(fd)
	fmt.Fprintln(r.prompt)
	if err != nil {
		return "", fmt.Errorf("read the password: %w", err)
	}
	return string(password), nil
}
