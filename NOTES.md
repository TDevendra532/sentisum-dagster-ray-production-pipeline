# NOTES — how I used Claude Code

I used Claude as a pair: I set the design, it accelerated the unfamiliar-API parts
(Dagster resources, Ray client semantics), and I verified everything by running it.

## Prompts that mattered (paraphrased)

1. *"Read the README, mocks.py and config.yaml. Summarise the three constraints and
   how each is meant to be enforced on Ray — don't write code yet."* — used it to
   build a mental model first.
2. *"Compose file: Ray head + scalable CPU workers + one worker advertising a custom
   resource gpu_slot=2. Head should take no work. Dashboard on 8265."*
3. *"For the LLM step: keep ≤8 tasks in flight cluster-wide with ray.wait, retry 429s
   with backoff inside the task. Why inside the task and not Ray max_retries?"* — the
   explanation (retrying ticket keeps its window slot; Ray retries would re-fire API
   calls invisibly) went into DECISIONS.md.
4. *"Add asset metadata so a reviewer can see counts/failures/retries/effective
   concurrency without reading code."*

## Where I trusted it

- Boilerplate: ConfigurableResource wiring, compose YAML anchors, metadata plumbing.
- Ray client vs driver-on-cluster trade-off (validated against Ray docs: Ray Data
  needs an on-cluster driver; plain core tasks don't).

## Where I corrected it

- First version had `from __future__ import annotations` in assets.py — Dagster's
  context type-hint validation breaks on stringified annotations. Caught it when the
  code location failed to load; removed it.
- It initially proposed a client-side semaphore for the GPU limit; I pushed to let
  the Ray scheduler enforce it via the custom resource instead — one less thing to
  get wrong, and it holds cluster-wide by construction.
- Tightened LLM task `num_cpus` to 0.1 after noticing effective concurrency sat
  below the cap on a small cluster (CPU reservations were the limiter, not the API).

## Verification

Ran end-to-end at N=40 and N=200 locally, checked: retries > 0 with 0 final
failures, embed concurrency pinned at 2 in the dashboard, no 429 storm, metadata
present on every step.
