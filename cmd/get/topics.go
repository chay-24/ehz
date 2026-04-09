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

func topicsCmd() cli.Command {
	return cli.Command{
		Name:  "topics",
		Short: "List all topics with partition and replication counts.",
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
				Name              string `json:"name"`
				Partitions        int    `json:"partitions"`
				ReplicationFactor int    `json:"replicationFactor"`
			}

			var rows []row

			err = conn.WithAdmin(ctx, env, func(admin *kadm.Client) error {
				topics, err := admin.ListTopics(ctx)
				if err != nil {
					return fmt.Errorf("listing topics: %w", err)
				}

				for _, td := range topics.Sorted() {
					rf := 0
					if len(td.Partitions) > 0 {
						rf = len(td.Partitions.Sorted()[0].Replicas)
					}
					rows = append(rows, row{
						Name:              td.Topic,
						Partitions:        len(td.Partitions),
						ReplicationFactor: rf,
					})
				}

				return nil
			})

			if err != nil {
				return err
			}

			sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

			if shared.OutputFormat(params) == "json" {
				return shared.PrintJSON(rows)
			}

			fmt.Printf("\nTopics in %s / %s\n\n", cfg.Current, env.Namespace)
			fmt.Printf("  %-48s  %10s  %7s\n", "NAME", "PARTITIONS", "REPLICAS")
			fmt.Printf("  %-48s  %10s  %7s\n", shared.Dashes(48), shared.Dashes(10), shared.Dashes(7))

			for _, r := range rows {
				fmt.Printf("  %-48s  %10d  %7d\n", r.Name, r.Partitions, r.ReplicationFactor)
			}

			fmt.Printf("\n%d topic(s)\n", len(rows))

			return nil
		},
	}
}
