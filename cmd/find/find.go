// Package find implements the 'ehz find' verb command.
package find

import (
	"context"
	"fmt"

	"github.com/joewhite86/cli"
)

// Find returns the 'find' verb command for searching Kafka resources.
func Find() *cli.Command {
	return &cli.Command{
		Name:  "find",
		Short: "Search for messages in a Kafka resource.",
		Commands: []cli.Command{
			topicCmd(),
		},
		Run: func(_ context.Context, _ cli.Params) error {
			fmt.Println("  ehz find topic <name> -w <expr>  — search by field value")

			return nil
		},
	}
}
