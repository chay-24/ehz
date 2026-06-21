// Package describe implements the 'ehz describe' verb command.
package describe

import (
	"context"
	"fmt"

	"github.com/joewhite86/cli"
)

// Describe returns the 'describe' verb command with all describable resources registered.
func Describe() *cli.Command {
	return &cli.Command{
		Name:  "describe",
		Short: "Show detailed information about a Kafka resource.",
		Commands: []cli.Command{
			topicCmd(),
			groupCmd(),
		},
		Run: func(_ context.Context, _ cli.Params) error {
			fmt.Println("  ehz describe topic <name>  — per-partition offsets and config")
			fmt.Println("  ehz describe group <name>  — per-partition lag")

			return nil
		},
	}
}
