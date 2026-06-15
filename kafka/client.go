// Package kafka provides helpers for connecting to a Kafka cluster through a
// local port-forward tunnel established by the openshift package.
package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// tunnelDialer returns a kgo.Opt that forces every broker connection
// regardless of the advertised internal hostname in Kafka metadata — through
// tunnelAddr (127.0.0.1:<localPort>). Required when reaching Kafka via
// 'oc port-forward' from outside the cluster.
func tunnelDialer(tunnelAddr string) kgo.Opt {
	d := &net.Dialer{Timeout: 10 * time.Second}
	return kgo.Dialer(func(ctx context.Context, network, _ string) (net.Conn, error) {
		return d.DialContext(ctx, network, tunnelAddr)
	})
}

// readOnlyOpts returns the kgo options that enforce read-only access.
//   - FetchIsolationLevel ReadCommitted: only see fully committed messages,
//     never read uncommitted transactional writes.
//   - DisableIdempotentWrite: explicitly no producer path. We never produce,
//     but this makes the intent visible and avoids needing IDEMPOTENT_WRITE
//     cluster permission.
//
// Note: AllowAutoTopicCreation is intentionally NOT called — its default is
// disabled, and calling it would enable accidental topic creation.
// Note: BlockRebalanceOnPoll is intentionally NOT used — it only applies when
// kgo.ConsumerGroup is set, which we never do, so offsets are never committed.
func readOnlyOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableIdempotentWrite(),
	}
}

// NewAdminClient creates a franz-go admin client connected through the
// port-forward tunnel at addr. All connections are forced through addr
// regardless of advertised broker hostnames. Read-only — no produce or
// commit operations are possible through this client.
// The caller must call Close() on the returned *kgo.Client when done.
func NewAdminClient(addr string) (*kadm.Client, *kgo.Client, error) {
	opts := append(readOnlyOpts(),
		kgo.SeedBrokers(addr),
		tunnelDialer(addr),
	)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("creating kafka admin client: %w", err)
	}

	return kadm.NewClient(cl), cl, nil
}

// NewConsumerClient creates a franz-go consumer client connected through the
// port-forward tunnel at addr. Read-only — it never commits offsets or joins
// a managed consumer group, so it has zero effect on the cluster's consumer
// group state.
// The caller must call Close() on the returned *kgo.Client when done.
func NewConsumerClient(addr string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := append(readOnlyOpts(),
		kgo.SeedBrokers(addr),
		tunnelDialer(addr),
	)

	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka consumer client: %w", err)
	}

	return cl, nil
}
