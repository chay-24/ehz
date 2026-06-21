// Package find implements the 'ehz find' verb command.
package find

import "github.com/joewhite86/cli"

// Find returns the 'find' verb command for searching Kafka resources.
func Find() *cli.Command {
	return &cli.Command{
		Name:  "find",
		Short: "Search for messages in a Kafka resource.",
		Commands: []cli.Command{
			topicCmd(),
		},
	}
}
