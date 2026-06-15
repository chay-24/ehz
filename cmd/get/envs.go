package get

import (
	"context"
	"fmt"
	"sort"

	"github.com/joewhite86/cli"

	"github.com/chay-24/ehz/cmd/shared"
	"github.com/chay-24/ehz/config"
)

func envsCmd() cli.Command {
	return cli.Command{
		Name:  "envs",
		Short: "Show all configured environments.",
		Flags: []cli.Flag{shared.OutputFlag},
		Run: func(_ context.Context, params cli.Params) error {
			cfgData, err := config.Load()
			if err != nil {
				return err
			}

			if shared.OutputFormat(params) == "json" {
				type envRow struct {
					Name      string `json:"name"`
					Cluster   string `json:"cluster"`
					Namespace string `json:"namespace"`
					Active    bool   `json:"active"`
				}

				names := make([]string, 0, len(cfgData.Environments))
				for name := range cfgData.Environments {
					names = append(names, name)
				}
				sort.Strings(names)

				rows := make([]envRow, 0, len(names))
				for _, name := range names {
					env := cfgData.Environments[name]
					rows = append(rows, envRow{
						Name:      name,
						Cluster:   env.Cluster,
						Namespace: env.Namespace,
						Active:    name == cfgData.Current,
					})
				}

				return shared.PrintJSON(rows)
			}

			if len(cfgData.Environments) == 0 {
				p, err := config.DefaultPath()
				if err != nil {
					return err
				}

				fmt.Printf("No environments configured. Edit %s to get started.\n", p)

				return nil
			}

			names := make([]string, 0, len(cfgData.Environments))
			for name := range cfgData.Environments {
				names = append(names, name)
			}
			sort.Strings(names)

			fmt.Println()
			for _, name := range names {
				env := cfgData.Environments[name]
				marker := "  "
				if name == cfgData.Current {
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
