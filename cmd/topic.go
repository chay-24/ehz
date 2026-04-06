package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/joewhite86/cli"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/chay-24/ehz/config"
	"github.com/chay-24/ehz/kafka"
)

// Topic returns the 'topic' command for inspecting Strimzi KafkaTopic resources.
func Topic() *cli.Command {
	return &cli.Command{
		Name:  "topic",
		Short: "List, describe, and consume Kafka topics.",
		Long:  "Reads topic metadata and messages from the Kafka cluster",
		Commands: []cli.Command{
			topicListCmd(),
			topicDescribeCmd(),
			topicConsumeCmd(),
			topicFindCmd(),
		},
		Run: func(_ context.Context, _ cli.Params) error {
			fmt.Println("  ehz topic list              — list all topics")
			fmt.Println("  ehz topic describe <topic>  — partition detail")
			fmt.Println("  ehz topic consume  <topic>  — stream messages")
			fmt.Println("  ehz topic find     <topic>  — search by field value")

			return nil
		},
	}
}

func topicListCmd() cli.Command {
	return cli.Command{
		Name:  "list",
		Short: "List all topics with partition and replication counts.",
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
				Name              string `json:"name"`
				Partitions        int    `json:"partitions"`
				ReplicationFactor int    `json:"replicationFactor"`
			}

			var rows []row

			err = withAdmin(ctx, env, func(admin *kadm.Client) error {
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

			if outputFormat(params) == "json" {
				return printJSON(rows)
			}

			fmt.Printf("\nTopics in %s / %s\n\n", cfg.Current, env.Namespace)
			fmt.Printf("  %-48s  %10s  %7s\n", "NAME", "PARTITIONS", "REPLICAS")
			fmt.Printf("  %-48s  %10s  %7s\n", dashes(48), dashes(10), dashes(7))

			for _, r := range rows {
				fmt.Printf("  %-48s  %10d  %7d\n", r.Name, r.Partitions, r.ReplicationFactor)
			}

			fmt.Printf("\n%d topic(s)\n", len(rows))

			return nil
		},
	}
}

func topicDescribeCmd() cli.Command {
	return cli.Command{
		Name:  "describe",
		Short: "Show per-partition detail for a topic.",
		Args: []cli.Arg{
			{Name: "topic", Description: "Topic name", Required: true},
		},
		Flags: []cli.Flag{outputFlag},
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

			err = withAdmin(ctx, env, func(admin *kadm.Client) error {
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

			if outputFormat(params) == "json" {
				return printJSON(detail)
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
				dashes(4), dashes(6), dashes(20), dashes(20), dashes(12), dashes(12), dashes(10))

			for _, p := range detail.Partitions {
				fmt.Printf("  %4d  %6d  %-20s  %-20s  %12d  %12d  %10d\n",
					p.Partition,
					p.Leader,
					formatInt32Slice(p.Replicas),
					formatInt32Slice(p.ISR),
					p.StartOffset,
					p.EndOffset,
					p.Messages,
				)
			}

			return nil
		},
	}
}

func topicConsumeCmd() cli.Command {
	return cli.Command{
		Name:  "consume",
		Short: "Stream messages from a topic.",
		Args: []cli.Arg{
			{Name: "topic", Description: "Topic name", Required: true},
		},
		Flags: []cli.Flag{
			{
				Short:       "b",
				Name:        "from-beginning",
				Description: "Start from the earliest available offset.",
			},
			{
				Short:       "p",
				Name:        "partition",
				HasValue:    true,
				Parser:      cli.Int32Parser,
				Description: "Consume only this partition (0-based index).",
				Default:     int32(-1),
			},
			{
				Name:        "offset",
				HasValue:    true,
				Parser:      int64Parser,
				Description: "Start offset within --partition (requires --partition).",
				Default:     int64(-1),
			},
			{
				Short:       "n",
				Name:        "max",
				HasValue:    true,
				Parser:      cli.Int32Parser,
				Description: "Maximum number of messages to print, then exit.",
				Default:     int32(0),
			},
			{
				Short:       "f",
				Name:        "follow",
				Description: "Keep consuming after --max is reached (like tail -f).",
			},
			{
				Short:       "w",
				Name:        "where",
				HasValue:    true,
				Description: `Filter JSON messages. Examples: "status=FAILED" or "meta.source=payments,status=FAILED~=err".`,
			},
			outputFlag,
		},
		Run: func(ctx context.Context, params cli.Params) error {
			topicName, _ := params["topic"].(string)
			fromBeginning := params["from-beginning"] == true
			partition, _ := params["partition"].(int32)
			offset, _ := params["offset"].(int64)
			maxMsgs, _ := params["max"].(int32)
			follow := params["follow"] == true
			whereExpr, _ := params["where"].(string)
			asJSON := outputFormat(params) == "json"

			filter, err := kafka.ParseFilter(whereExpr)
			if err != nil {
				return err
			}

			cfg, err := config.Load()
			if err != nil {
				return err
			}

			env, err := cfg.Active()
			if err != nil {
				return err
			}

			opts := buildConsumeOpts(topicName, fromBeginning, partition, offset)

			return withConsumer(ctx, env, opts, func(cl *kgo.Client) error {
				count := int32(0)
				for {
					fetches := cl.PollFetches(ctx)
					if fetches.IsClientClosed() || ctx.Err() != nil {
						return nil
					}

					var ferr error
					fetches.EachRecord(func(r *kgo.Record) {
						if ferr != nil {
							return
						}
						if !filter.Match(r.Value) {
							return
						}

						if err := printRecord(r, asJSON); err != nil {
							ferr = err

							return
						}

						count++
						if maxMsgs > 0 && count >= maxMsgs && !follow {
							ferr = errDone
						}
					})

					if ferr == errDone {
						return nil
					}

					if ferr != nil {
						return ferr
					}
				}
			})
		},
	}
}

func topicFindCmd() cli.Command {
	return cli.Command{
		Name:  "find",
		Short: "Search all partitions from the beginning for the first matching message.",
		Args: []cli.Arg{
			{Name: "topic", Description: "Topic name", Required: true},
		},
		Flags: []cli.Flag{
			{
				Short:       "w",
				Name:        "where",
				HasValue:    true,
				Required:    true,
				Description: `Filter expression, e.g. "orderId=abc-123" or "status=FAILED,source=payments".`,
			},
			{
				Short:       "p",
				Name:        "partition",
				HasValue:    true,
				Parser:      cli.Int32Parser,
				Description: "Limit search to this partition.",
				Default:     int32(-1),
			},
			outputFlag,
		},
		Run: func(ctx context.Context, params cli.Params) error {
			topicName, _ := params["topic"].(string)
			whereExpr, _ := params["where"].(string)
			partition, _ := params["partition"].(int32)
			asJSON := outputFormat(params) == "json"

			filter, err := kafka.ParseFilter(whereExpr)
			if err != nil {
				return err
			}

			cfg, err := config.Load()
			if err != nil {
				return err
			}

			env, err := cfg.Active()
			if err != nil {
				return err
			}

			opts := buildConsumeOpts(topicName, true, partition, -1)

			fmt.Fprintf(os.Stderr, "Scanning %s from beginning with filter: %s\n", topicName, whereExpr)

			return withConsumer(ctx, env, opts, func(cl *kgo.Client) error {
				for {
					fetches := cl.PollFetches(ctx)
					if fetches.IsClientClosed() || ctx.Err() != nil {
						return nil
					}

					var ferr error
					fetches.EachRecord(func(r *kgo.Record) {
						if ferr != nil {
							return
						}

						if !filter.Match(r.Value) {
							return
						}

						if err := printRecord(r, asJSON); err != nil {
							ferr = err

							return
						}

						ferr = errDone
					})

					if ferr == errDone {
						return nil
					}

					if ferr != nil {
						return ferr
					}
				}
			})
		},
	}
}

// errDone is a sentinel used to stop the consume loop cleanly.
var errDone = fmt.Errorf("done")

// buildConsumeOpts constructs kgo consumer options from the consume/find flags.
func buildConsumeOpts(topic string, fromBeginning bool, partition int32, offset int64) []kgo.Opt {
	if partition >= 0 {
		startOff := kgo.NewOffset().AtEnd()
		if fromBeginning {
			startOff = kgo.NewOffset().AtStart()
		}
		if offset >= 0 {
			startOff = kgo.NewOffset().At(offset)
		}
		return []kgo.Opt{
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {partition: startOff},
			}),
		}
	}

	resetOff := kgo.NewOffset().AtEnd()
	if fromBeginning {
		resetOff = kgo.NewOffset().AtStart()
	}
	return []kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(resetOff),
	}
}

// printRecord writes a single Kafka record to stdout.
// In JSON mode it emits a full envelope; otherwise it pretty-prints the value.
func printRecord(r *kgo.Record, asJSON bool) error {
	if asJSON {
		envelope := map[string]interface{}{
			"partition": r.Partition,
			"offset":    r.Offset,
			"timestamp": r.Timestamp.Format(time.RFC3339),
			"key":       string(r.Key),
			"value":     json.RawMessage(r.Value),
		}

		return printJSON(envelope)
	}

	// Pretty-print if the value is valid JSON, raw otherwise.
	var pretty interface{}
	if json.Unmarshal(r.Value, &pretty) == nil {
		out, _ := json.MarshalIndent(pretty, "", "  ")
		fmt.Printf("[p%d o%d] %s\n", r.Partition, r.Offset, out)
	} else {
		fmt.Printf("[p%d o%d] %s\n", r.Partition, r.Offset, r.Value)
	}

	return nil
}

// int64Parser parses an int64 flag value.
func int64Parser(val string) (interface{}, error) {
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return int64(-1), err
	}

	return n, nil
}

// formatInt32Slice renders a slice of broker IDs as "[1 2 3]".
func formatInt32Slice(ids []int32) string {
	if len(ids) == 0 {
		return "[]"
	}

	s := "["
	for i, id := range ids {
		if i > 0 {
			s += " "
		}
		s += strconv.Itoa(int(id))
	}

	return s + "]"
}
