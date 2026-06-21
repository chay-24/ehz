// Package get implements the 'ehz get' verb command.
package get

import (
	"context"
	"fmt"

	"github.com/joewhite86/cli"
)

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
		Run: func(_ context.Context, _ cli.Params) error {
			fmt.Println("  ehz get topics   — list all topics")
			fmt.Println("  ehz get brokers  — list broker pods")
			fmt.Println("  ehz get groups   — list consumer groups")
			fmt.Println("  ehz get envs     — list configured environments")

			return nil
		},
	}
}
