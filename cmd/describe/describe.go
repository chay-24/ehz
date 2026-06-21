// Package describe implements the 'ehz describe' verb command.
package describe

import "github.com/joewhite86/cli"

// Describe returns the 'describe' verb command with all describable resources registered.
func Describe() *cli.Command {
	return &cli.Command{
		Name:  "describe",
		Short: "Show detailed information about a Kafka resource.",
		Commands: []cli.Command{
			topicCmd(),
			groupCmd(),
		},
	}
}
