// Package openshift wraps the 'oc' CLI to run commands against a specific
// OpenShift cluster and namespace.
package openshift

import (
	"bytes"
	"fmt"
	"os/exec"
)

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
func Run(cluster, namespace string, args ...string) ([]byte, error) {
	base := []string{
		"--server=" + cluster,
		"--namespace=" + namespace,
	}
	cmd := exec.Command("oc", append(base, args...)...)

	var stderr bytes.Buffer

	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("oc %v: %s", args, stderr.String())
	}

	return out, nil
}
