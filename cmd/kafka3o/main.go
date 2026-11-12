// Command kafka3o is the Kafka3O command-line client.
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/cli"
	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
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
	getJSON := console.NewGetJSON(client)
	loadSession := func() (console.Session, error) { return console.LoadSession(sessionPath, time.Now) }
	sessionDeps := cli.SessionDependencies{LoadSession: loadSession, GetJSON: getJSON}
	root := cli.NewRootCommand(
		cli.NewLoginCommand(cli.LoginDependencies{SignIn: console.NewSignIn(client, signOut), SaveSession: console.SaveSession,
			SessionPath: sessionPath, ReadPassword: terminal.NewReadHiddenPassword(os.Stdin, os.Stderr)}),
		cli.NewLogoutCommand(loadSession, signOut, func() error { return console.DeleteSession(sessionPath) }),
		cli.NewWhoamiCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON)),
		cli.NewClustersCommand(sessionDeps, clusters.NewListClusterHealth(clusters.NewListAccessibleClusters(getJSON), clusters.NewReadClusterHealth(getJSON))),
	)
	err = root.Execute()
	cli.ForgetEndedSession(sessionPath, err)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o:", err)
	}
	os.Exit(cli.ExitCode(err))
}
