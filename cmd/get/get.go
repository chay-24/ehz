// Package get implements the 'ehz get' verb command.
package get

import "github.com/joewhite86/cli"

// Get returns the 'get' verb command with all listable resources registered.
func Get() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Short: "List Kafka resources.",
		Commands: []cli.Command{
			topicsCmd(),
			brokersCmd(),
			groupsCmd(),
			envsCmd(),
		},
	}
}
