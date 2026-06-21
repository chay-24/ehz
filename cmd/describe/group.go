package describe

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

func groupCmd() cli.Command {
	return cli.Command{
		Name:  "group",
		Short: "Show per-partition committed offsets and lag for a consumer group.",
		Args: []cli.Arg{
			{Name: "group", Description: "Consumer group name", Required: true},
		},
		Flags: []cli.Flag{shared.OutputFlag},
		Run: func(ctx context.Context, params cli.Params) error {
			groupName, _ := params["group"].(string)

			cfg, err := config.Load()
			if err != nil {
				return err
			}

			env, err := cfg.Active()
			if err != nil {
				return err
			}

			type partitionLag struct {
				Topic           string `json:"topic"`
				Partition       int32  `json:"partition"`
				CommittedOffset int64  `json:"committedOffset"`
				EndOffset       int64  `json:"endOffset"`
				Lag             int64  `json:"lag"`
			}

			type groupDetail struct {
				Group      string         `json:"group"`
				State      string         `json:"state"`
				Members    int            `json:"members"`
				TotalLag   int64          `json:"totalLag"`
				Partitions []partitionLag `json:"partitions"`
			}

			var detail groupDetail

			err = conn.WithAdmin(ctx, env, func(admin *kadm.Client) error {
				groups, err := admin.DescribeGroups(ctx, groupName)
				if err != nil {
					return fmt.Errorf("describing group: %w", err)
				}

				g, ok := groups[groupName]
				if !ok {
					return fmt.Errorf("group %q not found", groupName)
				}

				committedOffsets, err := admin.FetchOffsets(ctx, groupName)
				if err != nil {
					return fmt.Errorf("fetching committed offsets: %w", err)
				}

				// Collect all topics this group has committed offsets for.
				topicSet := make(map[string]struct{})
				committedOffsets.Each(func(o kadm.OffsetResponse) {
					topicSet[o.Topic] = struct{}{}
				})

				topics := make([]string, 0, len(topicSet))
				for t := range topicSet {
					topics = append(topics, t)
				}

				endOffsets, err := admin.ListEndOffsets(ctx, topics...)
				if err != nil {
					return fmt.Errorf("fetching end offsets: %w", err)
				}

				detail.Group = groupName
				detail.State = g.State
				detail.Members = len(g.Members)

				committedOffsets.Each(func(o kadm.OffsetResponse) {
					end, _ := endOffsets.Lookup(o.Topic, o.Partition)
					lag := int64(0)
					if end.Offset >= 0 && o.At >= 0 {
						lag = end.Offset - o.At
					}

					detail.TotalLag += lag
					detail.Partitions = append(detail.Partitions, partitionLag{
						Topic:           o.Topic,
						Partition:       o.Partition,
						CommittedOffset: o.At,
						EndOffset:       end.Offset,
						Lag:             lag,
					})
				})

				sort.Slice(detail.Partitions, func(i, j int) bool {
					if detail.Partitions[i].Topic != detail.Partitions[j].Topic {
						return detail.Partitions[i].Topic < detail.Partitions[j].Topic
					}

					return detail.Partitions[i].Partition < detail.Partitions[j].Partition
				})

				return nil
			})

			if err != nil {
				return err
			}

			if shared.OutputFormat(params) == "json" {
				return shared.PrintJSON(detail)
			}

			fmt.Printf("\nGroup:      %s\n", detail.Group)
			fmt.Printf("State:      %s\n", detail.State)
			fmt.Printf("Members:    %d\n", detail.Members)
			fmt.Printf("Total lag:  %d\n\n", detail.TotalLag)

			fmt.Printf("  %-40s  %9s  %17s  %10s  %6s\n",
				"TOPIC", "PARTITION", "COMMITTED-OFFSET", "END-OFFSET", "LAG")
			fmt.Printf("  %-40s  %9s  %17s  %10s  %6s\n",
				shared.Dashes(40), shared.Dashes(9), shared.Dashes(17), shared.Dashes(10), shared.Dashes(6))

			for _, p := range detail.Partitions {
				fmt.Printf("  %-40s  %9d  %17d  %10d  %6d\n",
					p.Topic, p.Partition, p.CommittedOffset, p.EndOffset, p.Lag)
			}

			return nil
		},
	}
}
