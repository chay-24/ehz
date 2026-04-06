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

// Broker returns the 'broker' command for inspecting Strimzi Kafka broker pods.
func Broker() *cli.Command {
	return &cli.Command{
		Name:  "broker",
		Short: "Inspect Kafka brokers in the active environment.",
		Long:  "Inspect Kafka brokers in the active environment.",
		Commands: []cli.Command{
			brokerListCmd(),
		},
	}
}

func brokerListCmd() cli.Command {
	return cli.Command{
		Name:  "list",
		Short: "List Kafka clusters and their broker pods in the active namespace.",
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

			clusterOut, err := openshift.Run(env.Cluster, env.Namespace,
				"get", "kafka",
				"-o", "json",
			)

			if err != nil {
				return err
			}

			var clusterList kafkaClusterList
			if err := json.Unmarshal(clusterOut, &clusterList); err != nil {
				return fmt.Errorf("parsing Kafka list: %w", err)
			}

			podOut, err := openshift.Run(env.Cluster, env.Namespace,
				"get", "pods",
				"-l", "strimzi.io/component-type=kafka",
				"-o", "json",
			)

			if err != nil {
				return err
			}

			var podList podListResponse
			if err := json.Unmarshal(podOut, &podList); err != nil {
				return fmt.Errorf("parsing broker pod list: %w", err)
			}

			sort.Slice(podList.Items, func(i, j int) bool {
				return podList.Items[i].Metadata.Name < podList.Items[j].Metadata.Name
			})

			if outputFormat(params) == "json" {
				return printJSON(map[string]interface{}{
					"clusters": clusterList.Items,
					"pods":     podList.Items,
				})
			}

			fmt.Printf("\nBrokers in %s / %s\n", cfg.Current, env.Namespace)

			if len(clusterList.Items) == 0 {
				fmt.Printf("\nNo Kafka CR found in namespace %q.\n", env.Namespace)
				fmt.Println("Check that Strimzi is installed and you are in the correct namespace.")

				return nil
			}

			for _, cluster := range clusterList.Items {
				fmt.Printf("\nCluster: %s\n", cluster.Metadata.Name)
				fmt.Printf("  Kafka version : %s\n", cluster.Status.KafkaVersion)
				fmt.Printf("  Broker count  : %d\n", len(podList.Items))
				fmt.Printf("  Cluster ready : %s\n", readyStatus(cluster.Status.Conditions))

				if len(cluster.Status.Listeners) > 0 {
					fmt.Printf("  Listeners:\n")
					for _, l := range cluster.Status.Listeners {
						fmt.Printf("    %-15s %s\n", l.Name+":", l.BootstrapServers)
					}
				}
			}

			if len(podList.Items) == 0 {
				fmt.Printf("\nNo broker pods found (label strimzi.io/component-type=kafka).\n")

				return nil
			}

			fmt.Printf("\nBroker pods:\n\n")
			fmt.Printf("  %-45s  %-8s  %s\n", "POD", "READY", "NODE")
			fmt.Printf("  %-45s  %-8s  %s\n", dashes(45), dashes(8), dashes(30))

			for _, pod := range podList.Items {
				readyCtr, totalCtr := podReadiness(pod.Status.ContainerStatuses)
				fmt.Printf("  %-45s  %-8s  %s\n",
					pod.Metadata.Name,
					fmt.Sprintf("%d/%d", readyCtr, totalCtr),
					pod.Spec.NodeName,
				)
			}

			fmt.Printf("\n%d broker pod(s)\n", len(podList.Items))

			return nil
		},
	}
}

type kafkaClusterList struct {
	Items []kafkaClusterItem `json:"items"`
}

type kafkaClusterItem struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Kafka struct {
			Replicas int `json:"replicas"`
		} `json:"kafka"`
	} `json:"spec"`
	Status struct {
		KafkaVersion string          `json:"kafkaVersion"`
		Conditions   []k8sCondition  `json:"conditions"`
		Listeners    []kafkaListener `json:"listeners"`
	} `json:"status"`
}

type kafkaListener struct {
	Name             string `json:"name"`
	BootstrapServers string `json:"bootstrapServers"`
}

type podListResponse struct {
	Items []podItem `json:"items"`
}

type podItem struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		ContainerStatuses []containerStatus `json:"containerStatuses"`
	} `json:"status"`
}

type containerStatus struct {
	Ready bool `json:"ready"`
}

// podReadiness returns the count of ready containers and the total container count.
func podReadiness(statuses []containerStatus) (ready, total int) {
	total = len(statuses)
	for _, s := range statuses {
		if s.Ready {
			ready++
		}
	}

	return
}
