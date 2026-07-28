# Gundi DLQ Processor

A command-line tool and operational runbook for recovering messages that
failed delivery in [Gundi](https://gundiservice.org) and ended up in a
dead-letter queue (DLQ).

## What's here

- **[Troubleshooting Runbook](runbook.md)** — start here when a connection's
  Activity Log reports messages "sent to dead-letter queue". Covers how
  dead-lettering actually works in Gundi, how to find where the messages
  went, how to diagnose why they failed, and how to fix the underlying
  problem before replaying anything.
- **[Reprocessing Reference](reprocessing.md)** — how to use `gundi_dlq.py`
  to replay (or purge) dead-lettered messages once the underlying issue is
  fixed.

## When to use this

Typical symptoms:

- A connection's Activity Log shows `observation_delivery_failed` events with
  *"Delivery retries exhausted... Message sent to dead-letter queue."*
- A connection's status is *unhealthy* with *"Errors were detected while
  pushing data to the destination"*.
- Data is missing at a destination (e.g. an EarthRanger site) for a window of
  time.

!!! warning "Dead letters expire"
    Pub/Sub subscriptions retain messages for a limited time (7 days by
    default). Dead-lettered messages that sit unprocessed past the retention
    window are **permanently lost**. Treat a growing dead-letter backlog as
    time-critical.

## Prerequisites

- Access to the Gundi GCP project (ask the platform team for the project ID
  and the required roles: Pub/Sub Subscriber, Pub/Sub Publisher, and read
  access to Cloud Run and Cloud Monitoring).
- `gcloud` CLI authenticated: `gcloud auth login` and
  `gcloud auth application-default login`.
- The tool installed: `pip install gundi-dlq` (or clone this repo and
  `pip install -e .`).
