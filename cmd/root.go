// Package cmd wires all ehz verb commands into the root command.
package cmd

import (
	"github.com/joewhite86/cli"

	"github.com/chay-24/ehz/cmd/consume"
	"github.com/chay-24/ehz/cmd/describe"
	"github.com/chay-24/ehz/cmd/find"
	"github.com/chay-24/ehz/cmd/get"
	"github.com/chay-24/ehz/cmd/use"
)

// Root returns the root ehz command with all verb commands registered.
func Root() *cli.Command {
	return &cli.Command{
		Name:    "ehz",
		Short:   "Explore and inspect your Kafka cluster.",
		Long:    "Explore and inspect your Kafka cluster.",
		Version: "0.1.0",
		Commands: []cli.Command{
			*get.Get(),
			*describe.Describe(),
			*consume.Consume(),
			*find.Find(),
			*use.Use(),
		},
	}
}
