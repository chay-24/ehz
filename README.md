# event-horizon

> *Where events become observable!*

`ehz` is the CLI for inspecting Strimzi Kafka messages. Tail topics, search for a specific message, and list topics.

## Install

```sh
git clone https://github.com/chay-24/ehz.git
cd ehz/
go build -o ehz .
```

## Configure

`ehz` reads `~/.ehz/config.yaml`:

```yaml
current: dev
environments:
  dev:
    cluster: https://api.dev.openshift.example.com:6443
    namespace: kafka-develop
  prod:
    cluster: https://api.prod.openshift.example.com:6443
    namespace: kafka-prod
```

You must already be logged in via `oc login` to each cluster URL.

## Commands
| Command | What it does |
| --- | --- |
| `ehz get topics` | List topics with partition & replication counts |
| `ehz get brokers` | List Kafka clusters and broker pods in the active namespace |
| `ehz get groups` | List consumer groups and their state |
| `ehz get envs` | Show configured environments |
| `ehz describe topic <name>` | Per-partition offsets, replicas, ISR, config |
| `ehz describe group <name>` | Per-partition committed offsets and lag |
| `ehz tree [cluster]` | Strimzi resource dependency tree
| `ehz consume topic <name>` | Stream messages from a topic |
| `ehz find topic <name> -w <expr>` | Scan from beginning for the first matching message |
| `ehz use env <name>` | switch the active environment |

All listing/describing commands accept `-o json` for machine output.

## Filtering message (`-w` / `--where`)

`consume` and `fidn` accept a comma-separated filter expression that runs against JSON message bodies.
Dot notation traverses nested fields.

```sh
ehz consume topic orders -w "status=FAILED"
ehz consume topic orders -w "meta.source=payments,status~=err"
ehz find topic orders -w "orderId=ae26ed07-f709-4688-8c10-b4e56"
```

Operators: `=` exact match, `~=` substring match.
