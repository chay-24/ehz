// Package consume implements the 'ehz consume' verb command.
package consume

import (
	"context"
	"fmt"

	"github.com/joewhite86/cli"
)

// Consume returns the 'consume' verb command with all consumable resources registered.
func Consume() *cli.Command {
	return &cli.Command{
		Name:  "consume",
		Short: "Stream messages from a Kafka resource.",
		Commands: []cli.Command{
			topicCmd(),
		},
		Run: func(_ context.Context, _ cli.Params) error {
			fmt.Println("  ehz consume topic <name>  — stream messages from a topic")

			return nil
		},
	}
}
