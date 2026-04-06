// Package cmd defines the top-level CLI commands for ehz.
package cmd

import "github.com/joewhite86/cli"

// Root returns the root ehz command with all sub-commands registered.
func Root() *cli.Command {
	return &cli.Command{
		Name:    "ehz",
		Short:   "Explore and inspect your Kafka cluster.",
		Long:    "Explore and inspect your Kafka cluster.",
		Version: "0.1.0",
		Commands: []cli.Command{
			*Config(),
			*Topic(),
			*Broker(),
			*Group(),
		},
	}
}
