package cli

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/backups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/clusters"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// BackupDependencies are what `kafka3o backup …` and `kafka3o restore` need.
type BackupDependencies struct {
	Session    SessionDependencies
	Accessible clusters.ListAccessibleClusters
	Start      backups.StartBackup
	Run        backups.RunBackupAction
}

// runBackupChange pauses, resumes or deletes a topic's backup with the UI's confirmation and result texts.
func runBackupChange(command *cobra.Command, deps BackupDependencies, cluster, topic, kind string, yes bool, reason string) error {
	if err := topics.ValidateTopicName(topic); err != nil {
		return err
	}
	session, err := deps.Session.LoadSession()
	if err != nil {
		return err
	}
	summary := map[string]string{
		backups.ActionPause:  fmt.Sprintf("Pause the backup of \"%s\" (new messages are not stored until it resumes)", topic),
		backups.ActionResume: fmt.Sprintf("Resume the backup of \"%s\"", topic),
		backups.ActionDelete: fmt.Sprintf("Delete the backup job for \"%s\" (the stored objects are kept; only the job stops)", topic),
	}[kind]
	sentReason, err := PrepareChange(command.Context(), session, deps.Accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
		Cluster: cluster, Yes: yes, Reason: reason, Summary: []string{summary},
	})
	if err != nil {
		return err
	}
	if err := deps.Run.Execute(command.Context(), session, cluster, topic, kind, sentReason); err != nil {
		return backupError(cluster, topic, err)
	}
	done := map[string]string{
		backups.ActionPause:  fmt.Sprintf("Pause requested for the backup of %s.", topic),
		backups.ActionResume: fmt.Sprintf("Resume requested for the backup of %s.", topic),
		backups.ActionDelete: fmt.Sprintf("The backup job for %s was deleted. Its stored objects are kept.", topic),
	}[kind]
	_, err = fmt.Fprintln(command.OutOrStdout(), done)
	return err
}
