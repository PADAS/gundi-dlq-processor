# Reprocessing Reference

`gundi_dlq.py` pulls messages from a dead-letter subscription and either
republishes them to a target topic (**reprocess**) or discards them
(**purge**). Non-matching messages are left in the queue untouched.

## Setup

```bash
git clone https://github.com/PADAS/gundi-dlq-processor
cd gundi-dlq-processor
poetry install
gcloud auth application-default login
```

You need Pub/Sub Subscriber on the source subscription and Pub/Sub Publisher
on the target topic.

## Reprocess

```bash
poetry run python gundi_dlq.py \
  --from-sub <dead-letter-subscription-id> \
  --to-topic <target-topic-id> \
  --project <gcp-project-id> \
  --reprocess \
  --connection <connection-id>
```

Each batch is pulled from the subscription; matching messages are republished
to the target topic (with their original data and attributes) and then acked.
Republishing resets the Pub/Sub publish time, so replayed messages pass the
dispatcher's age check — provided the dispatcher can deliver them within
`MAX_EVENT_AGE_SECONDS` (see the
[runbook](runbook.md#6-the-common-failure-mode-backlog-age-limit)).

## Purge

```bash
poetry run python gundi_dlq.py \
  --from-sub <dead-letter-subscription-id> \
  --project <gcp-project-id> \
  --purge \
  --connection <connection-id>
```

Acks matching messages without republishing them. **They are gone
permanently.** The tool asks for confirmation before starting.

## Flags

| Flag | Description |
|---|---|
| `--from-sub` | Subscription ID to pull from (required). |
| `--to-topic` | Topic ID to republish to (required with `--reprocess`). |
| `--project` | GCP project ID. |
| `--reprocess` / `--purge` | Exactly one is required. |
| `--connection` | Only process messages whose `data_provider_id` attribute matches. |
| `--gundi-id` / `--source-id` / `--system-id` | Additional per-message filters. |
| `--msg-type` / `--msg-type-exclude` | Include/exclude by event type (repeatable), e.g. `ObservationTransformedER`. |
| `--batch-size` | Messages pulled per iteration (default 100). |
| `--continue` | Keep polling when a batch comes back empty instead of prompting `Continue? [y/n]`. |

## Behavior and practical tips

- **Filtered-out messages are left unacked.** They reappear after the
  subscription's ack deadline, so when extracting one connection from a large
  shared DLQ you will churn through the same non-matching messages
  repeatedly. Expect many "excluded. Left in queue." lines — that's normal.
- **Large queues take hours.** Use `--batch-size 1000`, run with
  `--continue`, and keep the process alive in `tmux`/`screen`. Progress
  prints as `Total acknowledged/processed`.
- **Pulling without acking is safe.** If you only want to inspect a DLQ, use
  `gcloud pubsub subscriptions pull <sub> --limit=10 --format=json` (without
  `--auto-ack`) — messages redeliver after the ack deadline.
- **Watch the Activity Log while reprocessing.** If replayed messages
  immediately produce new dead-letter events, stop and go back to the
  [runbook](runbook.md) — the underlying delivery problem isn't fixed yet.
- **An empty pull doesn't mean an empty queue.** Pub/Sub pulls can return
  zero messages even when a backlog exists; the tool retries continuously
  with `--continue`.
