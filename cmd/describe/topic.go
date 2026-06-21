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

func topicCmd() cli.Command {
	return cli.Command{
		Name:  "topic",
		Short: "Show per-partition detail for a topic.",
		Args: []cli.Arg{
			{Name: "topic", Description: "Topic name", Required: true},
		},
		Flags: []cli.Flag{shared.OutputFlag},
		Run: func(ctx context.Context, params cli.Params) error {
			topicName, _ := params["topic"].(string)

			cfg, err := config.Load()
			if err != nil {
				return err
			}

			env, err := cfg.Active()
			if err != nil {
				return err
			}

			type partitionDetail struct {
				Partition   int32   `json:"partition"`
				Leader      int32   `json:"leader"`
				Replicas    []int32 `json:"replicas"`
				ISR         []int32 `json:"isr"`
				StartOffset int64   `json:"startOffset"`
				EndOffset   int64   `json:"endOffset"`
				Messages    int64   `json:"messages"`
			}

			type topicDetail struct {
				Name       string            `json:"name"`
				Partitions []partitionDetail `json:"partitions"`
				Config     map[string]string `json:"config,omitempty"`
			}

			var detail topicDetail

			err = conn.WithAdmin(ctx, env, func(admin *kadm.Client) error {
				topics, err := admin.ListTopics(ctx, topicName)
				if err != nil {
					return fmt.Errorf("describing topic: %w", err)
				}

				td, ok := topics[topicName]
				if !ok || td.Err != nil {
					return fmt.Errorf("topic %q not found", topicName)
				}

				startOffsets, err := admin.ListStartOffsets(ctx, topicName)
				if err != nil {
					return fmt.Errorf("fetching start offsets: %w", err)
				}

				endOffsets, err := admin.ListEndOffsets(ctx, topicName)
				if err != nil {
					return fmt.Errorf("fetching end offsets: %w", err)
				}

				configs, err := admin.DescribeTopicConfigs(ctx, topicName)
				if err == nil && len(configs) > 0 {
					detail.Config = make(map[string]string)
					for _, rc := range configs {
						for _, c := range rc.Configs {
							if c.Value != nil {
								detail.Config[c.Key] = *c.Value
							}
						}
					}
				}

				detail.Name = topicName
				for _, p := range td.Partitions.Sorted() {
					startLO, _ := startOffsets.Lookup(topicName, p.Partition)
					endLO, _ := endOffsets.Lookup(topicName, p.Partition)
					msgs := int64(0)
					if endLO.Offset >= 0 && startLO.Offset >= 0 {
						msgs = endLO.Offset - startLO.Offset
					}
					detail.Partitions = append(detail.Partitions, partitionDetail{
						Partition:   p.Partition,
						Leader:      p.Leader,
						Replicas:    p.Replicas,
						ISR:         p.ISR,
						StartOffset: startLO.Offset,
						EndOffset:   endLO.Offset,
						Messages:    msgs,
					})
				}

				return nil
			})

			if err != nil {
				return err
			}

			if shared.OutputFormat(params) == "json" {
				return shared.PrintJSON(detail)
			}

			fmt.Printf("\nTopic: %s\n", detail.Name)
			if len(detail.Config) > 0 {
				keys := make([]string, 0, len(detail.Config))
				for k := range detail.Config {
					keys = append(keys, k)
				}

				sort.Strings(keys)
				fmt.Println("\nConfig:")

				for _, k := range keys {
					fmt.Printf("  %-35s %s\n", k, detail.Config[k])
				}
			}

			fmt.Println("\nPartitions:")
			fmt.Printf("  %4s  %6s  %-20s  %-20s  %12s  %12s  %10s\n",
				"ID", "LEADER", "REPLICAS", "ISR", "START-OFFSET", "END-OFFSET", "MESSAGES")
			fmt.Printf("  %4s  %6s  %-20s  %-20s  %12s  %12s  %10s\n",
				shared.Dashes(4), shared.Dashes(6), shared.Dashes(20), shared.Dashes(20),
				shared.Dashes(12), shared.Dashes(12), shared.Dashes(10))

			for _, p := range detail.Partitions {
				fmt.Printf("  %4d  %6d  %-20s  %-20s  %12d  %12d  %10d\n",
					p.Partition,
					p.Leader,
					shared.FormatInt32Slice(p.Replicas),
					shared.FormatInt32Slice(p.ISR),
					p.StartOffset,
					p.EndOffset,
					p.Messages,
				)
			}

			return nil
		},
	}
}
