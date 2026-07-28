# Design: Gundi DLQ Troubleshooting Runbook on GitHub Pages

Date: 2026-07-28
Status: Approved

## Goal

Publish operational documentation for diagnosing and recovering dead-lettered
messages in Gundi, based on a real production incident (observations
dead-lettered for a Movebank → EarthRanger connection). Hosted as a public
GitHub Pages site built from this repo.

## Decisions

- **Scope**: full troubleshooting runbook (diagnosis → fix → reprocess), not
  just tool usage.
- **Location**: this repo (`PADAS/gundi-dlq-processor`), content in `docs/`.
- **Sensitivity**: the repo and Pages site are public. All examples use
  placeholders (`<gcp-project-id>`, `yoursite.pamdas.org`,
  `<site>-earth-dis-<uuid>-sub`). Real values are not repeated in the new
  docs. Scrubbing the existing README/CLI default project ID is out of scope.
- **Tooling**: MkDocs Material, deployed by a GitHub Actions workflow on push
  to `main` using the Pages artifact flow (no `gh-pages` branch).

## Components

1. `mkdocs.yml` — site config, Material theme, nav for three pages.
   `exclude_docs: superpowers/` so internal specs/plans under
   `docs/superpowers/` are not published.
2. `.github/workflows/docs.yml` — build with mkdocs, upload Pages artifact,
   deploy. Triggers on push to `main`; permissions `pages: write`,
   `id-token: write`.
3. `docs/index.md` — overview of the tool and when to use the runbook.
4. `docs/runbook.md` — the troubleshooting runbook:
   - How dead-lettering works in Gundi (application-level, not Pub/Sub
     dead-letter policy; `MAX_EVENT_AGE_SECONDS`; ER dispatchers publish
     observations to the observations dead-letter topic, not the dispatchers
     one).
   - Reading an Activity Log dead-letter entry.
   - Tracing where messages went: finding the destination's dispatcher
     subscription and Cloud Run service; checking backlogs and publish counts
     via Cloud Monitoring; reading dispatcher logs.
   - The common failure mode: large backlog + age limit ⇒ everything
     dead-letters; reprocessing alone just cycles messages back.
   - Fixes: scale the dispatcher (`maxScale`, concurrency); temporarily raise
     `MAX_EVENT_AGE_SECONDS` for intentional backfills, then revert; purge
     when data is unwanted.
   - Pitfalls: DLQ topics with no subscription drop messages; 7-day
     subscription retention silently expires dead letters; monitoring API
     errors can masquerade as "no data".
5. `docs/reprocessing.md` — `gundi_dlq.py` reference: auth, reprocess/purge,
   flags, filter semantics (non-matching messages left unacked and
   redelivered), practical tips (`--continue`, `--batch-size`, tmux).
6. `pyproject.toml` — add a `docs` Poetry dependency group with
   `mkdocs-material`.

## Post-merge manual step

Repo settings → Pages → Source: **GitHub Actions** (one-time).

## Out of scope

- Changes to `gundi_dlq.py` or the README.
- Multi-runbook navigation structure.
- Private/internal appendix with real production values.

## Verification

- `mkdocs build --strict` passes locally.
- Workflow deploys and the site renders after merge.
