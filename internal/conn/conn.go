// Package conn provides helpers for establishing a Kafka connection through
// an oc port-forward tunnel and running admin or consumer operations.
package conn

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/chay-24/ehz/config"
	"github.com/chay-24/ehz/kafka"
	"github.com/chay-24/ehz/openshift"
)

// WithAdmin resolves the Kafka bootstrap service, establishes a port-forward,
// creates a kadm admin client, and calls fn. The port-forward is torn down
// when fn returns.
func WithAdmin(ctx context.Context, env *config.Environment, fn func(*kadm.Client) error) error {
	addr, cancel, err := dial(ctx, env)
	if err != nil {
		return err
	}
	defer cancel()

	admin, cl, err := kafka.NewAdminClient(addr)
	if err != nil {
		return err
	}
	defer cl.Close()

	return fn(admin)
}

// WithConsumer resolves the Kafka bootstrap service, establishes a port-forward,
// creates a kgo consumer client with the provided options, and calls fn.
// The port-forward and client are torn down when fn returns.
func WithConsumer(ctx context.Context, env *config.Environment, opts []kgo.Opt, fn func(*kgo.Client) error) error {
	addr, cancel, err := dial(ctx, env)
	if err != nil {
		return err
	}
	defer cancel()

	cl, err := kafka.NewConsumerClient(addr, opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	return fn(cl)
}

// dial resolves the bootstrap service name and starts the port-forward.
func dial(ctx context.Context, env *config.Environment) (addr string, cancel func(), err error) {
	clusterName, err := resolveKafkaCluster(ctx, env)
	if err != nil {
		return "", nil, err
	}

	service := clusterName + "-kafka-bootstrap"

	localPort, cancel, err := openshift.PortForward(ctx, env.Cluster, env.Namespace, service, 9092)
	if err != nil {
		return "", nil, fmt.Errorf("port-forward to %s: %w", service, err)
	}

	return fmt.Sprintf("127.0.0.1:%d", localPort), cancel, nil
}

// resolveKafkaCluster returns the Strimzi Kafka CR name for the environment.
// If KafkaCluster is set in the config it is used directly; otherwise the
// first Kafka CR in the namespace is discovered via 'oc'.
func resolveKafkaCluster(ctx context.Context, env *config.Environment) (string, error) {
	if env.KafkaCluster != "" {
		return env.KafkaCluster, nil
	}

	out, err := openshift.Run(env.Cluster, env.Namespace,
		"get", "kafka",
		"-o", "jsonpath={.items[0].metadata.name}",
	)
	if err != nil {
		return "", fmt.Errorf("detecting Kafka cluster name: %w", err)
	}

	name := strings.TrimSpace(string(out))
	if name == "" {
		return "", fmt.Errorf(
			"no Kafka CR found in namespace %q — set kafka-cluster in ~/.ehz/config.yaml",
			env.Namespace,
		)
	}

	return name, nil
}
