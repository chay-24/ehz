// Package use implements the 'ehz use' verb command for switching active resources.
package use

import "github.com/joewhite86/cli"

// Use returns the 'use' verb command.
func Use() *cli.Command {
	return &cli.Command{
		Name:  "use",
		Short: "Switch the active resource.",
		Commands: []cli.Command{
			envCmd(),
		},
	}
}
