# Design: er-smart-sync-style build & publish

Date: 2026-07-28
Status: Approved

## Goal

Build and publish `gundi-dlq` the same way as other PADAS packages
(`er-smart-sync` from earthranger-smart-utils is the reference): setuptools +
setuptools-scm, tag-triggered release workflow with PyPI Trusted Publishing,
plus PR-triggered test runs.

## Decisions

- **Full conversion** from Poetry to `setuptools.build_meta` +
  `setuptools-scm>=8`; version derived from git tags (existing tags v0.1.0,
  v0.2.1 are compatible). `poetry.lock` deleted.
- **Fix latent packaging bug**: current wheel ships only the empty
  `gundi_dlq_processor/` stub — the real root module `gundi_dlq.py` is not
  packaged, so the installed console script is broken. Ship
  `py-modules = ["gundi_dlq"]`, delete the stub package.
- **Extras**: `dev = [pytest, ruff]`, `docs = [mkdocs-material]` (replaces the
  Poetry docs group).
- **Minimal CLI tests** with click's CliRunner (flag validation, --help,
  import) so the release test gate is meaningful.
- **PR-triggered tests** in addition to the release gate.

## Components

1. `pyproject.toml` — rewritten for setuptools + scm as above;
   `[project.scripts] gundi-dlq = "gundi_dlq:main"`.
2. `.github/workflows/release.yml` — copied from earthranger-smart-utils,
   adapted: on `v*.*.*` tag → checkout (fetch-depth 0) → install `.[dev]` →
   pytest → `python -m build` → verify wheel version == tag → publish via
   Trusted Publishing (environment `pypi`) → GitHub Release with artifacts.
3. `.github/workflows/tests.yml` — on pull_request and push to main:
   matrix Python 3.10/3.12, install `.[dev]`, pytest.
4. `tests/test_cli.py` — CliRunner tests for mutually-exclusive
   `--reprocess`/`--purge`, missing `--to-topic`, `--help`.
5. README + docs pages — replace `poetry install` / `poetry run` instructions
   with `pip install` / direct invocation.

## One-time manual steps (repo owner)

1. pypi.org → account → Publishing → add pending trusted publisher:
   project `gundi-dlq`, owner `PADAS`, repo `gundi-dlq-processor`,
   workflow `release.yml`, environment `pypi`.
2. GitHub repo → Settings → Environments → create `pypi`.
3. Release: `git tag v0.3.0 && git push origin v0.3.0`.

## Out of scope

- Runtime behavior changes.
- Lockfile-based reproducible dev environments (none of the reference repos
  keep one).

## Verification

- `python -m build` produces wheel+sdist; wheel contains `gundi_dlq.py`.
- `pytest` passes locally.
- Installed wheel's `gundi-dlq --help` works.
