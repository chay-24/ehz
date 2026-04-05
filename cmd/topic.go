package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/joewhite86/cli"

	"github.com/chay-24/ehz/config"
	"github.com/chay-24/ehz/openshift"
)

// Topic returns the 'topic' command for inspecting Strimzi KafkaTopic resources.
func Topic() *cli.Command {
	return &cli.Command{
		Name:  "topic",
		Short: "Inspect Kafka topics in the active environment.",
		Long:  "Reads KafkaTopic custom resources.",
		Commands: []cli.Command{
			topicListCmd(),
		},
		Run: func(_ context.Context, _ cli.Params) error {
			fmt.Println("  ehz topic list   — list all Kafka topics")
			return nil
		},
	}
}

func topicListCmd() cli.Command {
	return cli.Command{
		Name:  "list",
		Short: "List all KafkaTopic resources in the active namespace.",
		Run: func(_ context.Context, _ cli.Params) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}

			env, err := cfg.Active()
			if err != nil {
				return err
			}

			out, err := openshift.Run(env.Cluster, env.Namespace,
				"get", "kafkatopics",
				"-o", "json",
			)
			if err != nil {
				return err
			}

			var list kafkaTopicList
			if err := json.Unmarshal(out, &list); err != nil {
				return fmt.Errorf("parsing KafkaTopic list: %w", err)
			}

			if len(list.Items) == 0 {
				fmt.Printf("No KafkaTopic resources found in namespace %q.\n", env.Namespace)
				return nil
			}

			sort.Slice(list.Items, func(i, j int) bool {
				return list.Items[i].Metadata.Name < list.Items[j].Metadata.Name
			})

			fmt.Printf("\nTopics in %s / %s\n\n", cfg.Current, env.Namespace)
			fmt.Printf("  %-45s  %10s  %8s  %s\n", "NAME", "PARTITIONS", "REPLICAS", "READY")
			fmt.Printf("  %-45s  %10s  %8s  %s\n", dashes(45), dashes(10), dashes(8), dashes(5))

			for _, t := range list.Items {
				ready := readyStatus(t.Status.Conditions)
				fmt.Printf("  %-45s  %10d  %8d  %s\n",
					t.Metadata.Name,
					t.Spec.Partitions,
					t.Spec.Replicas,
					ready,
				)
			}

			fmt.Printf("\n%d topic(s)\n", len(list.Items))

			return nil
		},
	}
}

type kafkaTopicList struct {
	Items []kafkaTopicItem `json:"items"`
}

type kafkaTopicItem struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Partitions int `json:"partitions"`
		Replicas   int `json:"replicas"`
	} `json:"spec"`
	Status struct {
		Conditions []k8sCondition `json:"conditions"`
	} `json:"status"`
}

// k8sCondition is a standard Kubernetes status condition (type + status pair).
type k8sCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

// readyStatus scans a condition slice and returns "Ready", "NotReady", or "Unknown".
func readyStatus(conditions []k8sCondition) string {
	for _, c := range conditions {
		if c.Type == "Ready" {
			if c.Status == "True" {
				return "Ready"
			}
			return "NotReady"
		}
	}
	return "Unknown"
}

// dashes returns a string of n dash characters, used for table separators.
func dashes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '-'
	}
	return string(b)
}
