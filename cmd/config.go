package cmd

import (
	"context"
	"fmt"
	"sort"

	"github.com/joewhite86/cli"

	"github.com/chay-24/ehz/config"
)

// Config returns the 'config' command for viewing and switching environments.
func Config() *cli.Command {
	return &cli.Command{
		Name:  "config",
		Short: "View or switch environments.",
		Long:  "Reads ~/.ehz/config.yaml. Edit that file directly to add environments.",
		Commands: []cli.Command{
			configViewCmd(),
			configUseCmd(),
		},
		Run: func(_ context.Context, _ cli.Params) error {
			p, err := config.DefaultPath()
			if err != nil {
				return err
			}

			fmt.Printf("Config: %s\n\n", p)
			fmt.Println("  ehz config view        — show environments")
			fmt.Println("  ehz config use <env>   — switch active environment")

			return nil
		},
	}
}

func configViewCmd() cli.Command {
	return cli.Command{
		Name:  "view",
		Short: "Show all configured environments.",
		Run: func(_ context.Context, _ cli.Params) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}

			if len(cfg.Environments) == 0 {
				p, err := config.DefaultPath()
				if err != nil {
					return err
				}

				fmt.Printf("No environments configured. Edit %s to get started.\n", p)

				return nil
			}

			names := make([]string, 0, len(cfg.Environments))
			for name := range cfg.Environments {
				names = append(names, name)
			}
			sort.Strings(names)

			fmt.Println()
			for _, name := range names {
				env := cfg.Environments[name]
				marker := "  "
				if name == cfg.Current {
					marker = "▶ "
				}

				fmt.Printf("%s%s\n", marker, name)
				fmt.Printf("    cluster:   %s\n", env.Cluster)
				fmt.Printf("    namespace: %s\n", env.Namespace)
				fmt.Println()
			}

			return nil
		},
	}
}

func configUseCmd() cli.Command {
	return cli.Command{
		Name:  "use",
		Short: "Switch the active environment.",
		Args: []cli.Arg{
			{Name: "env", Description: "Environment name", Required: true},
		},
		Run: func(_ context.Context, params cli.Params) error {
			name, _ := params["env"].(string)
			cfg, err := config.Load()
			if err != nil {
				return err
			}

			if err := cfg.Use(name); err != nil {
				return err
			}

			fmt.Printf("Switched to %s\n", name)

			return nil
		},
	}
}
