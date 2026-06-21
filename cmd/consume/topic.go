package consume

import (
	"context"

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
				Parser:      shared.Int64Parser,
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
			shared.OutputFlag,
		},
		Run: func(ctx context.Context, params cli.Params) error {
			topicName, _ := params["topic"].(string)
			fromBeginning := params["from-beginning"] == true
			partition, _ := params["partition"].(int32)
			offset, _ := params["offset"].(int64)
			maxMsgs, _ := params["max"].(int32)
			follow := params["follow"] == true
			whereExpr, _ := params["where"].(string)
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

			opts := shared.BuildConsumeOpts(topicName, fromBeginning, partition, offset)

			return conn.WithConsumer(ctx, env, opts, func(cl *kgo.Client) error {
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

						if err := shared.PrintRecord(r, asJSON); err != nil {
							ferr = err

							return
						}

						count++
						if maxMsgs > 0 && count >= maxMsgs && !follow {
							ferr = shared.ErrDone
						}
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
