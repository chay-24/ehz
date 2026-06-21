// Package consume implements the 'ehz consume' verb command.
package consume

import "github.com/joewhite86/cli"

// Consume returns the 'consume' verb command with all consumable resources registered.
func Consume() *cli.Command {
	return &cli.Command{
		Name:  "consume",
		Short: "Stream messages from a Kafka resource.",
		Commands: []cli.Command{
			topicCmd(),
		},
	}
}
