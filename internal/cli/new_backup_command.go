package cli

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/misterkafkagod/Kafka3O-cli/internal/backups"
	"github.com/misterkafkagod/Kafka3O-cli/internal/messages"
	"github.com/misterkafkagod/Kafka3O-cli/internal/topics"
)

// NewBackupCommand groups starting, pausing, resuming and deleting a topic's continuous backup.
func NewBackupCommand(deps BackupDependencies) *cobra.Command {
	var yes bool
	var reason string
	var settings backups.Settings
	backup := &cobra.Command{Use: "backup", Short: "Start, pause, resume or delete a topic's continuous backup"}
	start := &cobra.Command{
		Use:   "start <cluster> <topic>",
		Short: "Start a continuous backup of a topic to the cluster's backup storage",
		Args:  cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			if err := topics.ValidateTopicName(args[1]); err != nil {
				return err
			}
			if err := backups.ValidateBackupSettings(settings); err != nil {
				return err
			}
			var chosen *backups.Settings
			if command.Flags().Changed("start-from") || command.Flags().Changed("compression") || command.Flags().Changed("records-per-file") {
				chosen = &settings
			}
			effective := topics.BackupSettings{StartFrom: settings.StartFrom, Compression: settings.Compression, RecordsPerFile: settings.RecordsPerFile}
			if effective.StartFrom == "" {
				effective.StartFrom = "earliest"
			}
			if effective.Compression == "" {
				effective.Compression = "gzip"
			}
			session, err := deps.Session.LoadSession()
			if err != nil {
				return err
			}
			sentReason, err := PrepareChange(command.Context(), session, deps.Accessible, bufio.NewReader(command.InOrStdin()), command.ErrOrStderr(), ChangeRequest{
				Cluster: args[0], Yes: yes, Reason: reason,
				Summary: []string{
					fmt.Sprintf("Start a continuous backup of \"%s\"", args[1]),
					"settings: " + describeBackupSettings(&effective),
					"its messages are copied to the cluster's backup storage until the backup is deleted",
				},
			})
			if err != nil {
				return err
			}
			created, err := deps.Start.Execute(command.Context(), session, args[0], args[1], chosen, sentReason)
			if err != nil {
				return backupError(args[0], args[1], err)
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Backup started for %s. Messages are stored in %s.\n", args[1], messages.EscapeTerminalText(created.Location))
			return err
		},
	}
	start.Flags().StringVar(&settings.StartFrom, "start-from", "", "earliest (the oldest message, default) or latest (messages from now on)")
	start.Flags().StringVar(&settings.Compression, "compression", "", "gzip (default), zstd, snappy or none")
	start.Flags().IntVar(&settings.RecordsPerFile, "records-per-file", 0, "split stored files after this many records (0 = no limit)")
	addChangeFlags(start, &yes, &reason)
	backup.AddCommand(start)
	for _, kind := range []struct{ name, short string }{
		{backups.ActionPause, "Pause a topic's backup; new messages are not stored until it resumes"},
		{backups.ActionResume, "Resume a paused backup"},
		{backups.ActionDelete, "Delete a topic's backup job; the stored objects are kept"},
	} {
		action := &cobra.Command{
			Use:   kind.name + " <cluster> <topic>",
			Short: kind.short,
			Args:  cobra.ExactArgs(2),
			RunE: func(command *cobra.Command, args []string) error {
				return runBackupChange(command, deps, args[0], args[1], kind.name, yes, reason)
			},
		}
		addChangeFlags(action, &yes, &reason)
		backup.AddCommand(action)
	}
	return backup
}
