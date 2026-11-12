// Command kafka3o is the Kafka3O command-line client.
package main

import (
	"fmt"
	"os"

	"github.com/misterkafkagod/Kafka3O-cli/internal/cli"
)

func main() {
	root := cli.NewRootCommand()
	err := root.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o:", err)
	}
	os.Exit(cli.ExitCode(err))
}
