package cmd

import (
	"context"
	"fmt"
	"sort"

	"github.com/joewhite86/cli"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/chay-24/ehz/config"
)

// Group returns the 'group' command for inspecting Kafka consumer groups.
func Group() *cli.Command {
	return &cli.Command{
		Name:  "group",
		Short: "List and describe Kafka consumer groups.",
		Long:  "Reads consumer group metadata and per-partition lag from the Kafka cluster.",
		Commands: []cli.Command{
			groupListCmd(),
			groupDescribeCmd(),
		},
		Run: func(_ context.Context, _ cli.Params) error {
			fmt.Println("  ehz group list              — list all consumer groups")
			fmt.Println("  ehz group describe <group>  — show per-partition lag")
			return nil
		},
	}
}

func groupListCmd() cli.Command {
	return cli.Command{
		Name:  "list",
		Short: "List all consumer groups and their state.",
		Flags: []cli.Flag{outputFlag},
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

			err = withAdmin(ctx, env, func(admin *kadm.Client) error {
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

			if outputFormat(params) == "json" {
				return printJSON(rows)
			}

			fmt.Printf("\nConsumer groups in %s / %s\n\n", cfg.Current, env.Namespace)
			fmt.Printf("  %-50s  %-12s  %7s\n", "GROUP", "STATE", "MEMBERS")
			fmt.Printf("  %-50s  %-12s  %7s\n", dashes(50), dashes(12), dashes(7))
			for _, r := range rows {
				fmt.Printf("  %-50s  %-12s  %7d\n", r.Group, r.State, r.Members)
			}

			fmt.Printf("\n%d group(s)\n", len(rows))

			return nil
		},
	}
}

func groupDescribeCmd() cli.Command {
	return cli.Command{
		Name:  "describe",
		Short: "Show per-partition committed offsets and lag for a consumer group.",
		Args: []cli.Arg{
			{Name: "group", Description: "Consumer group name", Required: true},
		},
		Flags: []cli.Flag{outputFlag},
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

			err = withAdmin(ctx, env, func(admin *kadm.Client) error {
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

			if outputFormat(params) == "json" {
				return printJSON(detail)
			}

			fmt.Printf("\nGroup:      %s\n", detail.Group)
			fmt.Printf("State:      %s\n", detail.State)
			fmt.Printf("Members:    %d\n", detail.Members)
			fmt.Printf("Total lag:  %d\n\n", detail.TotalLag)

			fmt.Printf("  %-40s  %9s  %17s  %10s  %6s\n",
				"TOPIC", "PARTITION", "COMMITTED-OFFSET", "END-OFFSET", "LAG")
			fmt.Printf("  %-40s  %9s  %17s  %10s  %6s\n",
				dashes(40), dashes(9), dashes(17), dashes(10), dashes(6))

			for _, p := range detail.Partitions {
				fmt.Printf("  %-40s  %9d  %17d  %10d  %6d\n",
					p.Topic, p.Partition, p.CommittedOffset, p.EndOffset, p.Lag)
			}

			return nil
		},
	}
}
