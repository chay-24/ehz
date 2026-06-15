// Package use implements the 'ehz use' verb command for switching active resources.
package use

import (
	"context"
	"fmt"

	"github.com/joewhite86/cli"
)

// Use returns the 'use' verb command.
func Use() *cli.Command {
	return &cli.Command{
		Name:  "use",
		Short: "Switch the active resource.",
		Commands: []cli.Command{
			envCmd(),
		},
		Run: func(_ context.Context, _ cli.Params) error {
			fmt.Println("  ehz use env <name>  — switch active environment")
			return nil
		},
	}
}
