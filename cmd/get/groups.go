package get

import (
	"context"
	"fmt"
	"sort"

	"github.com/joewhite86/cli"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/chay-24/ehz/cmd/shared"
	"github.com/chay-24/ehz/config"
	"github.com/chay-24/ehz/internal/conn"
)

func groupsCmd() cli.Command {
	return cli.Command{
		Name:  "groups",
		Short: "List all consumer groups and their state.",
		Flags: []cli.Flag{shared.OutputFlag},
		Run: func(ctx context.Context, params cli.Params) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}

			env, err := cfg.Active()
			if err != nil {
				return err
			}

			type row struct {
				Group   string `json:"group"`
				State   string `json:"state"`
				Members int    `json:"members"`
			}

			var rows []row

			err = conn.WithAdmin(ctx, env, func(admin *kadm.Client) error {
				groups, err := admin.DescribeGroups(ctx)
				if err != nil {
					return fmt.Errorf("listing consumer groups: %w", err)
				}

				for _, g := range groups.Sorted() {
					rows = append(rows, row{
						Group:   g.Group,
						State:   g.State,
						Members: len(g.Members),
					})
				}

				return nil
			})

			if err != nil {
				return err
			}

			sort.Slice(rows, func(i, j int) bool { return rows[i].Group < rows[j].Group })

			if shared.OutputFormat(params) == "json" {
				return shared.PrintJSON(rows)
			}

			fmt.Printf("\nConsumer groups in %s / %s\n\n", cfg.Current, env.Namespace)
			fmt.Printf("  %-50s  %-12s  %7s\n", "GROUP", "STATE", "MEMBERS")
			fmt.Printf("  %-50s  %-12s  %7s\n", shared.Dashes(50), shared.Dashes(12), shared.Dashes(7))
			for _, r := range rows {
				fmt.Printf("  %-50s  %-12s  %7d\n", r.Group, r.State, r.Members)
			}

			fmt.Printf("\n%d group(s)\n", len(rows))

			return nil
		},
	}
}
