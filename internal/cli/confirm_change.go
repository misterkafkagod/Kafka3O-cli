package cli

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
)

// ConfirmChange shows a Console change that belongs to no cluster (accounts, access, sessions) and asks before
// it is sent, unless yes; anything but "y" cancels.
func ConfirmChange(input *bufio.Reader, prompt io.Writer, summary []string, yes bool) error {
	fmt.Fprintln(prompt, "Change:")
	for _, line := range summary {
		fmt.Fprintf(prompt, "  %s\n", line)
	}
	if yes {
		return nil
	}
	fmt.Fprint(prompt, "Proceed? [y/N] ")
	line, _ := input.ReadString('\n')
	if answer := strings.ToLower(strings.TrimSpace(line)); answer != "y" && answer != "yes" {
		return console.ErrCanceled
	}
	return nil
}
