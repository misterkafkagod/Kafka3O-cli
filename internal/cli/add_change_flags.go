package cli

import "github.com/spf13/cobra"

// addChangeFlags adds --yes and --reason, which every change command shares.
func addChangeFlags(command *cobra.Command, yes *bool, reason *string) {
	command.Flags().BoolVarP(yes, "yes", "y", false, "do not ask for confirmation")
	command.Flags().StringVar(reason, "reason", "", "change reason (required on Production clusters; asked for when missing)")
}
