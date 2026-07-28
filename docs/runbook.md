# Troubleshooting Runbook: Dead-Lettered Messages

This runbook walks the full arc of a dead-letter incident: understanding what
the Activity Log is telling you, finding where the messages actually went,
diagnosing the failure, fixing the cause, and only then replaying the
messages.

Throughout, replace placeholders like `<gcp-project-id>` with your real
values (from the GCP console or your platform team).

## 1. How dead-lettering works in Gundi

Two different mechanisms can send a message to a dead-letter topic, and they
land in **different places**:

1. **Pub/Sub dead-letter policy** — a subscription-level setting
   (`deadLetterPolicy`) that moves a message after N failed delivery
   attempts. Integration action-runner subscriptions typically use this,
   dead-lettering to per-integration topics (e.g.
   `<integration>-actions-dead-letter`).
2. **Application-level dead-lettering** — the dispatcher service itself
   decides to give up and *publishes* the message to a dead-letter topic in
   code. This is what produces the Activity Log error:

    > Delivery retries exhausted (message older than 43200 seconds). Message
    > sent to dead-letter queue.

    The EarthRanger dispatcher checks the Pub/Sub **publish time** of each
    incoming message against `MAX_EVENT_AGE_SECONDS` (default 43200 = 12
    hours). Anything older is rejected and published to a dead-letter topic.

!!! danger "The topic name is not what you expect"
    The ER dispatcher's `DEAD_LETTER_TOPIC` environment variable is **not**
    where observations go. Observations are published to the
    **observations dead-letter topic** (e.g. `observations-dead-letter`),
    with its own subscription (e.g. `observations-dead-letter-sub`). If you
    pull from the dispatchers' dead-letter subscription and find nothing,
    this is why. Confirm by reading the dispatcher's logs (step 4) — it logs
    the exact topic it publishes to.

## 2. Read the Activity Log entry

A `observation_delivery_failed` event contains everything you need to start:

| Field | What it tells you |
|---|---|
| `integration.id` | The **connection / data provider ID** — used to filter DLQ messages (`data_provider_id` attribute). |
| `observation.destination_id` | The **destination integration ID** — identifies the dispatcher service and subscription. |
| `title` (site URL) | Which destination site deliveries were failing against. |
| `details.error` | Which mechanism fired. "message older than N seconds" = the age check described above. |
| `created_at` | When dead-lettering happened. Compare against the 7-day retention clock. |

## 3. Find the dispatcher's subscription and service

Per-destination dispatcher resources embed the destination ID (truncated) in
their names. Find them:

```bash
# The dispatcher's input subscription and topic
gcloud pubsub subscriptions list --project=<gcp-project-id> \
  --format="csv[no-heading](name,topic,deadLetterPolicy.deadLetterTopic)" \
  | grep <destination-id-prefix>

# The Cloud Run service running the dispatcher
gcloud run services list --project=<gcp-project-id> \
  --format="value(metadata.name)" | grep <destination-id-prefix>
```

For EarthRanger destinations the pattern looks like
`<site>-earth-dis-<destination-uuid-prefix>-sub` subscribed to
`<site>-earthran-<hash>-topic`.

## 4. Establish where the messages are

Check backlog sizes across all subscriptions with Cloud Monitoring (the
console's Pub/Sub page also shows this per-subscription):

```bash
TOKEN=$(gcloud auth print-access-token)
END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
START=$(date -u -v-15M +%Y-%m-%dT%H:%M:%SZ)   # -d '15 min ago' on Linux
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://monitoring.googleapis.com/v3/projects/<gcp-project-id>/timeSeries?filter=metric.type%3D%22pubsub.googleapis.com%2Fsubscription%2Fnum_undelivered_messages%22&interval.startTime=$START&interval.endTime=$END&pageSize=500"
```

!!! warning "Two ways this query lies to you"
    1. **Errors look like empty results.** If your token is expired the API
       returns a 401 error object, and a script that only reads `timeSeries`
       prints "no data". Always check for an `error` key in the response.
    2. **Pagination.** With many subscriptions, the largest backlog can be
       beyond the first page. Follow `nextPageToken`, or query the specific
       subscription you care about by adding
       `AND resource.labels.subscription_id="<sub-id>"` to the filter.

Then look at both queues:

- **The dead-letter subscription** (e.g. `observations-dead-letter-sub`) —
  how many messages, and how old is `oldest_unacked_message_age`? If it's
  approaching 604800 (7 days), messages are about to expire permanently.
- **The dispatcher's own subscription** — a large backlog here is the smoking
  gun for the failure mode in step 6.

Sample the dead-letter queue (pulling without acking is non-destructive) to
confirm your connection's messages are in it:

```bash
gcloud pubsub subscriptions pull observations-dead-letter-sub \
  --project=<gcp-project-id> --limit=100 --format=json \
  | grep -c '<connection-id>'
```

Read the dispatcher's logs to see what it's doing right now:

```bash
gcloud logging read 'resource.type="cloud_run_revision"
  AND resource.labels.service_name="<dispatcher-service-name>"' \
  --project=<gcp-project-id> --freshness=15m --limit=30
```

Log lines like `Event is too old (timestamp = ...) and will be sent to
dead-letter.` and `Sending observation to PubSub topic <topic>..` tell you
exactly which topic dead letters go to, and which timestamp the age check is
rejecting.

## 5. Check the dead-letter topic has a subscription

A Pub/Sub topic with **no subscription silently drops** every message
published to it. Before assuming messages are recoverable:

```bash
gcloud pubsub topics list-subscriptions <dead-letter-topic> --project=<gcp-project-id>
```

If this lists nothing, the messages reported as "sent to dead-letter queue"
no longer exist. Attach a subscription immediately to stop the ongoing loss,
and re-send the data from the source for the window already lost.

## 6. The common failure mode: backlog + age limit

When a destination receives a large burst (e.g. a historical backfill) or the
destination site is slow/down for a while, a queue builds up on the
dispatcher's subscription. Dispatchers are Cloud Run services with modest
scaling limits (`autoscaling.knative.dev/maxScale`, `containerConcurrency`),
so the queue drains slowly. Once the backlog is deeper than
`MAX_EVENT_AGE_SECONDS` of throughput, **every** message reaching the
dispatcher is already "too old" and gets dead-lettered — including messages
you replay from the DLQ, since they rejoin the back of the same queue.

Do the math before acting:

```text
time-to-drain = backlog_size / observed_drain_rate
```

If `time-to-drain > MAX_EVENT_AGE_SECONDS`, replaying is futile until you
change one of those numbers:

1. **Scale the dispatcher up.** Raise max instances (and optionally
   concurrency) on the Cloud Run service:

    ```bash
    gcloud run services update <dispatcher-service-name> \
      --max-instances=10 --region=<region> --project=<gcp-project-id>
    ```

    Watch the destination site — it receives everything the dispatcher can
    now push.

2. **Raise the age limit temporarily** so queued messages deliver instead of
   dead-lettering. 604800 = 7 days:

    ```bash
    gcloud run services update <dispatcher-service-name> \
      --update-env-vars MAX_EVENT_AGE_SECONDS=604800 \
      --region=<region> --project=<gcp-project-id>
    ```

    This alone rescues the messages still on the dispatcher's subscription —
    no DLQ replay needed for those. **Revert to the default after the
    backlog clears**, otherwise genuinely stale data will be delivered long
    after it stops being useful.

3. **Purge instead**, if the queued data is unwanted (e.g. an accidental
   backfill): `gcloud pubsub subscriptions seek <sub> --time=<now>` discards
   everything pending on that subscription. This is destructive and affects
   *all* providers delivering to that destination — check what else flows
   through it first.

!!! tip "Intentional backfills"
    If you are deliberately loading historical data, raise
    `MAX_EVENT_AGE_SECONDS` and scale the dispatcher **before** starting the
    backfill, and size the age limit to cover the expected drain time.

## 7. Replay the dead letters

Only after the dispatcher is healthy (backlog draining, deliveries
succeeding, age limit covering the drain time), replay the dead-lettered
messages back through the dispatcher's input topic:

```bash
gundi-dlq \
  --from-sub observations-dead-letter-sub \
  --to-topic <site>-earthran-<hash>-topic \
  --reprocess \
  --connection <connection-id> \
  --batch-size 1000 \
  --continue
```

See the [Reprocessing Reference](reprocessing.md) for flag details and
behavior. Verify success in the connection's Activity Log (delivery events
instead of new dead-letter events) and by spot-checking data at the
destination.

## 8. Post-incident checklist

- [ ] Revert `MAX_EVENT_AGE_SECONDS` to its default.
- [ ] Revert dispatcher scaling if it was raised beyond steady-state needs.
- [ ] Confirm the dead-letter subscription backlog is back to ~0.
- [ ] Confirm connection status returned to healthy in the Gundi portal.
- [ ] If any dead-letter topic had no subscription, or messages expired past
      retention: record the data-loss window and re-send from the source if
      possible.
