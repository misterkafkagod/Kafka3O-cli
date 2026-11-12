// Command kafka3o is the Kafka3O command-line client.
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/cli"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/terminal"
)

func main() {
	client := &http.Client{Timeout: 30 * time.Second}
	sessionPath, err := console.SessionPath(os.UserConfigDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o:", err)
		os.Exit(cli.ExitUsage)
	}
	signOut := console.NewSignOut(client)
	root := cli.NewRootCommand(
		cli.NewLoginCommand(cli.LoginDependencies{SignIn: console.NewSignIn(client, signOut), SaveSession: console.SaveSession,
			SessionPath: sessionPath, ReadPassword: terminal.NewReadHiddenPassword(os.Stdin, os.Stderr)}),
	)
	err = root.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o:", err)
	}
	os.Exit(cli.ExitCode(err))
}
