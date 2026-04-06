// Package kafka provides helpers for connecting to a Kafka cluster through a
// local port-forward tunnel established by the openshift package.
package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewAdminClient creates a franz-go admin client connected to addr
// (typically "127.0.0.1:<localPort>" from a port-forward).
// The caller must call Close() on the returned *kgo.Client when done.
func NewAdminClient(addr string) (*kadm.Client, *kgo.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		return nil, nil, fmt.Errorf("creating kafka client: %w", err)
	}

	return kadm.NewClient(cl), cl, nil
}

// NewConsumerClient creates a franz-go consumer client connected to addr with
// the provided options layered on top of the seed broker setting.
// The caller must call Close() on the returned *kgo.Client when done.
func NewConsumerClient(addr string, opts ...kgo.Opt) (*kgo.Client, error) {
	all := append([]kgo.Opt{kgo.SeedBrokers(addr)}, opts...)
	cl, err := kgo.NewClient(all...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka consumer: %w", err)
	}

	return cl, nil
}
