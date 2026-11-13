// Command kafka3o is the Kafka3O command-line client.
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/misterkafkagod/Kafka3O-cli/internal/accounts"
	"github.com/misterkafkagod/Kafka3O-cli/internal/audit"
	"github.com/misterkafkagod/Kafka3O-cli/internal/backups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/cli"
	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/connect"
	"github.com/misterkafkagod/Kafka3O-cli/internal/console"
	"github.com/misterkafkagod/Kafka3O-cli/internal/groups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/metrics"
	"github.com/misterkafkagod/Kafka3O-cli/internal/terminal"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
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
	sendJSON := console.NewSendJSON(client)
	loadSession := func() (console.Session, error) { return console.LoadSession(sessionPath, time.Now) }
	sessionDeps := cli.SessionDependencies{LoadSession: loadSession, GetJSON: getJSON}
	auditCommand := cli.NewAuditCommand(sessionDeps, audit.NewListAuditEvents(getJSON))
	auditCommand.AddCommand(cli.NewAuditExportCommand(sessionDeps, audit.NewExportAuditEvents(console.NewGetCSV(client))))
	backupDeps := cli.BackupDependencies{Session: sessionDeps, Accessible: clusters.NewListAccessibleClusters(getJSON),
		Start: backups.NewStartBackup(sendJSON), Run: backups.NewRunBackupAction(sendJSON), Restore: backups.NewStartRestore(sendJSON),
		Wait: backups.NewWaitForRestore(backups.NewListBackups(clusters.NewListAccessibleClusters(getJSON), getJSON), backups.NewSleep(), 5*time.Second)}
	root := cli.NewRootCommand(
		cli.NewLoginCommand(cli.LoginDependencies{SignIn: console.NewSignIn(client, signOut), SaveSession: console.SaveSession,
			SessionPath: sessionPath, ReadPassword: terminal.NewReadHiddenPassword(os.Stdin, os.Stderr)}),
		cli.NewLogoutCommand(loadSession, signOut, func() error { return console.DeleteSession(sessionPath) }),
		cli.NewWhoamiCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON)),
		cli.NewClustersCommand(sessionDeps, clusters.NewListClusterHealth(clusters.NewListAccessibleClusters(getJSON), clusters.NewReadClusterHealth(getJSON))),
		cli.NewTopicsCommand(sessionDeps, topics.NewListTopicsAcrossClusters(clusters.NewListAccessibleClusters(getJSON), topics.NewListClusterTopics(getJSON))),
		cli.NewTopicCommand(sessionDeps, topics.NewDescribeTopic(clusters.NewListAccessibleClusters(getJSON), getJSON)),
		cli.NewBrokersCommand(sessionDeps, clusters.NewListBrokers(clusters.NewListAccessibleClusters(getJSON), getJSON)),
		cli.NewBrokerConfigCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON), clusters.NewReadBrokerConfig(getJSON)),
		cli.NewMetricsCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON), metrics.NewReadMetrics(getJSON)),
		cli.NewMessagesCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON), messages.NewReadMessages(getJSON)),
		cli.NewSearchCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON), messages.NewSearchMessages(getJSON)),
		cli.NewGroupsCommand(sessionDeps, groups.NewListGroups(clusters.NewListAccessibleClusters(getJSON), getJSON)),
		cli.NewGroupCommand(sessionDeps, groups.NewListGroups(clusters.NewListAccessibleClusters(getJSON), getJSON)),
		cli.NewProduceCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON), messages.NewProduceMessage(sendJSON)),
		cli.NewCreateTopicCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON), topics.NewCreateTopic(sendJSON)),
		cli.NewDeleteTopicCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON), topics.NewDeleteTopic(sendJSON)),
		cli.NewConfigCommand(cli.ConfigurationDependencies{Session: sessionDeps, Accessible: clusters.NewListAccessibleClusters(getJSON),
			Read: clusters.NewReadConfiguration(getJSON), Change: clusters.NewChangeConfiguration(sendJSON)}),
		cli.NewPartitionsCommand(cli.PartitionDependencies{Session: sessionDeps, Accessible: clusters.NewListAccessibleClusters(getJSON),
			Describe: topics.NewDescribeTopic(clusters.NewListAccessibleClusters(getJSON), getJSON), Increase: topics.NewIncreasePartitions(sendJSON),
			Reassign: topics.NewReassignPartition(sendJSON)}),
		cli.NewReassignmentsCommand(sessionDeps, clusters.NewListAccessibleClusters(getJSON), topics.NewListReassignments(getJSON)),
		cli.NewConnectorsCommand(sessionDeps, connect.NewListConnectors(clusters.NewListAccessibleClusters(getJSON), getJSON)),
		cli.NewConnectorCommand(cli.ConnectorDependencies{Session: sessionDeps, Accessible: clusters.NewListAccessibleClusters(getJSON),
			Run: connect.NewRunConnectorAction(sendJSON)}),
		cli.NewBackupsCommand(sessionDeps, backups.NewListBackups(clusters.NewListAccessibleClusters(getJSON), getJSON)),
		cli.NewBackupCommand(backupDeps),
		cli.NewRestoreCommand(backupDeps),
		auditCommand,
		cli.NewAccountsCommand(sessionDeps, accounts.NewListAccounts(getJSON)),
		cli.NewAccountCommand(cli.AccountDependencies{Session: sessionDeps, ReadPassword: terminal.NewReadHiddenPassword(os.Stdin, os.Stderr),
			Find: accounts.NewFindAccount(accounts.NewListAccounts(getJSON)), Create: accounts.NewCreateAccount(sendJSON),
			Reset: accounts.NewResetPassword(sendJSON), SetEnabled: accounts.NewSetAccountEnabled(sendJSON)}),
		cli.NewResetOffsetsCommand(cli.ResetDependencies{Session: sessionDeps, Accessible: clusters.NewListAccessibleClusters(getJSON),
			Preview: groups.NewPreviewOffsetReset(getJSON), Apply: groups.NewApplyOffsetReset(sendJSON)}),
	)
	err = root.Execute()
	cli.ForgetEndedSession(sessionPath, err)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka3o:", err)
	}
	os.Exit(cli.ExitCode(err))
}
