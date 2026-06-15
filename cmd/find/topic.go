package find

import (
	"context"
	"fmt"
	"os"

	"github.com/joewhite86/cli"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/chay-24/ehz/cmd/shared"
	"github.com/chay-24/ehz/config"
	"github.com/chay-24/ehz/internal/conn"
	"github.com/chay-24/ehz/kafka"
)

func topicCmd() cli.Command {
	return cli.Command{
		Name:  "topic",
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
			shared.OutputFlag,
		},
		Run: func(ctx context.Context, params cli.Params) error {
			topicName, _ := params["topic"].(string)
			whereExpr, _ := params["where"].(string)
			partition, _ := params["partition"].(int32)
			asJSON := shared.OutputFormat(params) == "json"

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

			opts := shared.BuildConsumeOpts(topicName, true, partition, -1)

			fmt.Fprintf(os.Stderr, "Scanning %s from beginning with filter: %s\n", topicName, whereExpr)

			return conn.WithConsumer(ctx, env, opts, func(cl *kgo.Client) error {
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

						if err := shared.PrintRecord(r, asJSON); err != nil {
							ferr = err

							return
						}

						ferr = shared.ErrDone
					})

					if ferr == shared.ErrDone {
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
