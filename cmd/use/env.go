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
		Flags: []cli.Flag{
			{
				Short:       "c",
				Name:        "cluster",
				HasValue:    true,
				Description: "OpenShift API server URL. Creates or updates the environment.",
			},
			{
				Short:       "n",
				Name:        "namespace",
				HasValue:    true,
				Description: "OpenShift project. Creates or updates the environment.",
			},
		},
		Run: func(_ context.Context, params cli.Params) error {
			name, _ := params["name"].(string)
			cluster, _ := params["cluster"].(string)
			namespace, _ := params["namespace"].(string)

			cfgData, err := config.Load()
			if err != nil {
				return err
			}

			if cluster == "" && namespace == "" {
				if err := cfgData.Use(name); err != nil {
					return err
				}

				fmt.Printf("Switched to %s\n", name)

				return nil
			}

			env, existed := cfgData.Environments[name]
			if !existed && (cluster == "" || namespace == "") {
				return fmt.Errorf("creating environment %q requires both --cluster and --namespace", name)
			}

			// Only overwrite what was passed, so a partial update keeps the rest.
			if cluster != "" {
				env.Cluster = cluster
			}

			if namespace != "" {
				env.Namespace = namespace
			}

			if err := cfgData.Upsert(name, env); err != nil {
				return err
			}

			// Upsert only sets Current when nothing was active yet.
			if cfgData.Current != name {
				if err := cfgData.Use(name); err != nil {
					return err
				}
			}

			fmt.Printf("Switched to %s\n", name)

			return nil
		},
	}
}
