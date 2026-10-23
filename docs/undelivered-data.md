# Runbook: Data Received but Never Delivered

Use this runbook when a provider's data reached Gundi but never arrived at the
destination, and **there may be no dead-letter error at all**. The
[Troubleshooting Runbook](runbook.md) covers the case where the Activity Log
already reports messages "sent to dead-letter queue".

The rule: **find out where the messages stopped before you try to recover
them.** Each stopping point has a different recovery. Some recoveries make
things worse if they're done in the wrong order, for example seeking before
raising the dispatcher's age limit, or replaying while the destination still
rejects the data.

Replace placeholders like `<gcp-project-id>` with real values from the GCP
console or your platform team.

## 1. Confirm what arrived

A connection can show **healthy** with no Activity Log errors while none of
its data is delivered (see [case A](#case-a-topic-without-a-subscription)).
Don't rely on status.

The Gundi API logs one `gundi_api.v2.data_received` line per item it accepts.
Count them for the connection and time window:

```bash
gcloud logging read 'jsonPayload.data_provider_id="<connection-id>"
  AND jsonPayload.message:"gundi_api.v2.data_received"
  AND timestamp>="<start>"' \
  --project <gcp-project-id> --limit 200000 \
  --format='value(timestamp,jsonPayload.external_source_id,jsonPayload.recorded_at)' > received.tsv

wc -l < received.tsv                                          # items received
awk '{print $2}' received.tsv | sort | uniq -c                # per device
awk '{print substr($1,1,13)}' received.tsv | sort | uniq -c   # per hour
```

!!! note
    These log lines carry metadata only (device, `recorded_at`, source). They
    tell you what to recover, but **they can't be replayed**.

## 2. Check the destination's resources

Per-destination dispatcher resources include the first part of the
destination ID in their names ([runbook step 3](runbook.md#3-find-the-dispatchers-subscription-and-service)).
Check that the topic, the subscription and the Cloud Run service all exist,
then check **when each was created**:

```bash
gcloud logging read 'protoPayload.serviceName="pubsub.googleapis.com"
  AND protoPayload.resourceName:"<site>" AND timestamp>="<start>"' \
  --project <gcp-project-id> --format=json \
  | jq -r '.[] | "\(.timestamp) \(.protoPayload.methodName) status=\(.protoPayload.status.code // 0)"'
```

* A topic created **before** the data arrived and a subscription created
  **after** it is the case A signature.
* `status=6` is `ALREADY_EXISTS`. If a redeploy's `CreateTopic` fails this
  way, an earlier, partial deploy had already created the topic.

## 3. Find where the messages stopped

### Case A: topic without a subscription

* **Signature:**
  * The topic existed while data flowed, but the subscription didn't.
  * The routing service logged no publish errors.
  * Nothing is in any dead-letter queue.
* **Why:** Pub/Sub **discards** messages published to a topic with no
  subscription, unless the *topic* has message retention. The publisher gets
  a success response, so nothing logs an error.
* **Check the topic's own retention:**

  ```bash
  gcloud pubsub topics describe <topic> --project <gcp-project-id> --format=json
  # look for messageRetentionDuration
  ```

  !!! danger "Topic retention is not subscription retention"
      Subscriptions always have a `messageRetentionDuration` (7 days by
      default). That does **not** let you seek back before the subscription
      existed. Only a `messageRetentionDuration` on the **topic** does.
      Describe the topic on its own, so its output can't be confused with a
      subscription's.

* **If the topic has retention:** recover with a
  [seek](#41-seek-the-dispatchers-subscription).
* **If it has none:** the messages are gone from Pub/Sub. Don't assume the
  routing service's input topic still has copies. Check it for topic
  retention, and check its subscription for `retainAckedMessages`. If neither
  is set, [re-send from the source](#43-re-send-from-the-source).

### Case B: the routing service could not publish

* **Signature:** the routing service's logs show publish errors against the
  destination topic, such as `ClientResponseError: 404` (topic missing) or a
  5xx, on `.../topics/<topic>:publish`.
* **Why:**
  * The routing service re-raises the error, and Pub/Sub keeps redelivering
    the message to it.
  * Once the message is older than the routing service's
    `MAX_EVENT_AGE_SECONDS`, it's published to the **routing dead-letter
    topic**. This limit can be as short as a few minutes, so check the
    service.
  * The dead-letter log says only that the message was too old, not why it
    failed.
* **Gotchas:**
  * The routing dead-letter topic may have **more than one subscription**,
    each with a full copy. Replay from only one, or every message is
    delivered twice.
  * Messages in it don't carry a `data_provider_id` *attribute*. In v2
    messages it's in the payload (`payload.data_provider_id`), and v1
    messages don't have it at all. So `--connection` matches nothing. Filter
    with `--gundi-id`, `--system-id` or `--msg-type` instead.

### Case C: the dispatcher could not deliver

* **Signature:** the dispatcher logs show delivery errors or `Event is too
  old`. The connection's Activity Log usually shows
  `observation_delivery_failed`.
* **Common causes:**
  * The destination rejects the data. For example, EarthRanger returns `400`
    with `"Value '<event_type>' does not exist."` after the event type was
    deleted on the site.
  * The dispatcher service is down, or can't scale (`no available instance`).
  * A backlog older than the dispatcher's `MAX_EVENT_AGE_SECONDS`
    ([runbook step 6](runbook.md#6-the-common-failure-mode-backlog-age-limit)).
* **Where they go:** the dispatcher picks the dead-letter topic by stream
  type. See the table in
  [runbook step 1](runbook.md#1-how-dead-lettering-works-in-gundi).
  Messages there carry `data_provider_id`, so `--connection` works.

## 4. Recover

Fix the cause first: recreate the missing resources, restore the event types,
or scale the dispatcher. **Confirm that new data is being delivered** before
you replay anything.

### 4.1 Seek the dispatcher's subscription

Use this for case A when the topic has retention, or when messages are still
within the dispatcher subscription's own retention.

```bash
# 1. Seek redelivers messages with their ORIGINAL publish time, so raise the age limit first
gcloud run services update <dispatcher-service-name> \
  --update-env-vars MAX_EVENT_AGE_SECONDS=604800 \
  --region <region> --project <gcp-project-id>

# 2. Wait until the new revision serves 100% of traffic
gcloud run services describe <dispatcher-service-name> --region <region> \
  --project <gcp-project-id> --format=json | jq '.status.traffic'

# 3. Seek to just before the first lost message
gcloud pubsub subscriptions seek <dispatcher-subscription> \
  --time=<timestamp> --project <gcp-project-id>
```

After delivery is verified ([step 5](#5-verify)), revert
`MAX_EVENT_AGE_SECONDS`. A seek redelivers **everything** published after the
timestamp, including messages that were already delivered.

### 4.2 Replay a dead-letter queue

Use this for cases B and C. Follow the [Reprocessing Reference](reprocessing.md).

| Source | Target topic | Filter |
|---|---|---|
| A dispatcher dead-letter subscription (case C) | The dispatcher's input topic | `--connection <connection-id>` |
| The routing dead-letter subscription (case B), **one subscription only** | The routing service's input topic | `--gundi-id` / `--system-id` / `--msg-type` (not `--connection`) |

* **Before replaying, check that nobody else is already replaying.** Look for
  ack activity or expiring ack deadlines on the dead-letter subscription
  ([step 5](#5-verify)). Concurrent replays race each other.
* **Make sure the destination accepts the data before replaying.** Replayed
  messages that fail retry until they're too old, then get dead-lettered
  again.

### 4.3 Re-send from the source

Use this when the messages no longer exist anywhere: case A without topic
retention, or messages past their retention. Either the provider re-sends, or
the integration's pull action is re-run.

!!! warning "Gundi API deduplication"
    The API discards an item if it has seen an identical payload from the
    same connection and source within its duplicate-check window.

    * The window is a deployment setting, **30 minutes by default**. Confirm
      the value for your environment.
    * The check **refreshes the window even when it finds a duplicate**.
      Don't re-send the same records twice in quick succession.
    * If the window hasn't passed yet, wait, or make each payload differ
      slightly, for example with an extra field under `additional`.

Re-sent items get new Gundi IDs and new system event IDs. The routing service
and the dispatcher won't treat them as duplicates.

* **Test with one small device first.** Verify it ([step 5](#5-verify)), then
  send the rest.
* **For historical backfills,** raise the dispatcher's
  `MAX_EVENT_AGE_SECONDS` until the backlog has drained.

## 5. Verify

```bash
# Sum a Pub/Sub metric in 10-minute buckets
q() { F=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$1")
  curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://monitoring.googleapis.com/v3/projects/<gcp-project-id>/timeSeries?filter=$F&interval.startTime=<start>&interval.endTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)&aggregation.alignmentPeriod=600s&aggregation.perSeriesAligner=ALIGN_SUM&aggregation.crossSeriesReducer=REDUCE_SUM" \
  | jq -r 'if .error then "ERROR \(.error.message)" else [.timeSeries[0].points[]? | "\(.interval.endTime[11:16])=\(.value.int64Value)"] | reverse | join(" ") end'; }

q 'metric.type="pubsub.googleapis.com/topic/send_request_count" AND resource.labels.topic_id="<topic>"'
q 'metric.type="pubsub.googleapis.com/subscription/ack_message_count" AND resource.labels.subscription_id="<subscription>"'
q 'metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" AND resource.labels.subscription_id="<subscription>"'
```

* **Count messages with `topic/send_request_count`.**
  `topic/send_message_operation_count` measures something different and can
  report a higher number.
* **Messages may be batches.** A dispatcher message with `batch: true`
  carries `batch_count` items. Sum `batch_count` over the dispatcher's
  received messages and compare it with the `data_received` count from
  [step 1](#1-confirm-what-arrived).
* Look for success responses from the destination (`201 Created` for
  EarthRanger), and no new `too old` or dead-letter log lines.
* Spot-check the data at the destination itself.

## 6. Close out

- [ ] Every `MAX_EVENT_AGE_SECONDS` you changed is reverted.
- [ ] Dispatcher scaling reverted, if you raised it.
- [ ] No replay process is still running against the dead-letter
      subscription.
- [ ] The connection's new data is being delivered.
- [ ] Any unrecoverable loss is recorded: time window, devices and count.
- [ ] The original cause is written down, such as a deployment that created
      the topic but not the subscription. File a follow-up if it can recur.
