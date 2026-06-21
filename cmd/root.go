// Package cmd wires all ehz verb commands into the root command.
package cmd

import (
	"github.com/joewhite86/cli"

	"github.com/chay-24/ehz/cmd/consume"
	"github.com/chay-24/ehz/cmd/describe"
	"github.com/chay-24/ehz/cmd/find"
	"github.com/chay-24/ehz/cmd/get"
	"github.com/chay-24/ehz/cmd/tree"
	"github.com/chay-24/ehz/cmd/use"
)

// Root returns the root ehz command with all verb commands registered.
func Root() *cli.Command {
	return &cli.Command{
		Name:    "ehz",
		Short:   "Explore and inspect your Strimzi Kafka cluster on OpenShift.",
		Long:    "Explore and inspect your StrimziKafka cluster on OpenShift.",
		Version: "0.1.0",
		Commands: []cli.Command{
			*get.Get(),
			*describe.Describe(),
			*tree.Tree(),
			*consume.Consume(),
			*find.Find(),
			*use.Use(),
		},
	}
}
