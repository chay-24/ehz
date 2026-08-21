// Package openshift wraps the 'oc' CLI to run commands against a specific
// OpenShift cluster and namespace.
package openshift

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"time"
)

const waitDelay = 2 * time.Second

// Strimzi resource types for use with 'oc' commands.
const (
	ResourceKafka         = "kafka.kafka.strimzi.io"
	ResourceKafkaNodePool = "kafkanodepool.kafka.strimzi.io"
	ResourceKafkaTopic    = "kafkatopic.kafka.strimzi.io"
	ResourceKafkaUser     = "kafkauser.kafka.strimzi.io"
	ResourceStrimziPodSet = "strimzipodset.core.strimzi.io"
)

// Run executes an 'oc' command against the given cluster and namespace,
// returning the combined stdout output. Stderr is captured and returned
// as part of the error if the command fails.
//
// Cancelling ctx kills the 'oc' subprocess, so an interrupt takes effect
// immediately instead of waiting for the cluster round trip to finish.
func Run(ctx context.Context, cluster, namespace string, args ...string) ([]byte, error) {
	base := []string{
		"--server=" + cluster,
		"--namespace=" + namespace,
	}
	cmd := exec.CommandContext(ctx, "oc", append(base, args...)...)
	cmd.WaitDelay = waitDelay

	var stderr bytes.Buffer

	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("oc %v: %w", args, ctxErr)
		}

		return nil, fmt.Errorf("oc %v: %s", args, stderr.String())
	}

	return out, nil
}
