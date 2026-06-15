package use

import (
	"context"
	"fmt"

	"github.com/joewhite86/cli"

	"github.com/chay-24/ehz/config"
)

func envCmd() cli.Command {
	return cli.Command{
		Name:  "env",
		Short: "Switch the active environment.",
		Args: []cli.Arg{
			{Name: "name", Description: "Environment name", Required: true},
		},
		Run: func(_ context.Context, params cli.Params) error {
			name, _ := params["name"].(string)

			cfgData, err := config.Load()
			if err != nil {
				return err
			}

			if err := cfgData.Use(name); err != nil {
				return err
			}

			fmt.Printf("Switched to %s\n", name)

			return nil
		},
	}
}
