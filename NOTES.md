# NOTES — how I used Claude Code

Dagster and Ray were both new to me, which is the point of this exercise. My
approach was: **I own the design and the "why", Claude accelerates the "how"** —
the unfamiliar API surface (Dagster resources/assets, Ray client vs driver, the
scheduling model). I treated every suggestion as a draft to verify by running, not
as truth. The bugs and design corrections below are the honest record of that.

## How I worked through it

1. **Understand before writing.** I first had Claude read `README.md`, `mocks.py`
   and `config.yaml` and just *explain* the three constraints and how each is meant
   to be enforced on Ray — no code. This gave me a mental model of what the exercise
   actually tests (infra/ops judgement, not ML) before I touched anything.
2. **Cluster first, then wire in.** Stood up the Ray topology in `docker-compose.yml`
   (head with `--num-cpus=0`, scalable CPU workers, one GPU worker advertising the
   custom `gpu_slot` resource), got the dashboard on `:8265`, *then* moved the three
   heavy assets onto it one at a time.
3. **One constraint at a time.** featurized (chunking) -> embedded (gpu_slot gating)
   -> summarised (concurrency window + retries), running and checking each in the
   Dagster metadata and Ray dashboard before moving on.
4. **Observability last.** Added asset metadata (counts, failures, retries,
   effective LLM concurrency, throughput) so the pipeline is legible without reading
   code — which is explicitly what the README asks reviewers to check.

## Prompts that mattered (paraphrased)

- *"Read the README, mocks.py and config.yaml. Explain the three constraints and how
  each is meant to be enforced on Ray. Don't write code yet."*
- *"Compose file: Ray head that takes no work + scalable CPU workers + one worker
  advertising custom resource gpu_slot=2. Dashboard on 8265. Worker count easy to
  change for scale tests."*
- *"LLM step: keep at most 8 tasks in flight cluster-wide using ray.wait, retry
  transient 429s with backoff. Should the retry live inside the task or in Ray's
  max_retries? Explain the trade-off."*
- *"Enforce the GPU limit through the Ray scheduler via the gpu_slot resource, not a
  client-side semaphore — walk me through why that's more robust."*
- *"Add Dagster asset metadata so a reviewer can see per-step counts, failures,
  retries and effective LLM concurrency without reading the code."*

## Where I trusted Claude

- Boilerplate I'd otherwise look up: `ConfigurableResource` wiring, docker-compose
  YAML anchors, the metadata plumbing.
- Confirming the Ray-client-vs-on-cluster-driver trade-off — I used plain Ray core
  tasks so the Dagster driver can stay a client; Ray Data would have needed the
  driver on the cluster. I sanity-checked this against Ray's docs rather than taking
  it on faith.

## Where I corrected / overrode Claude

- **`from __future__ import annotations` broke Dagster.** The first `assets.py` had
  it, and Dagster's context type-hint validation rejects stringified annotations —
  the code location wouldn't load. I caught it on the first run and removed the
  import. Good reminder that "it compiles" != "it loads".
- **GPU limit: scheduler, not semaphore.** The initial idea was a client-side
  semaphore around the embed calls. I pushed for the Ray scheduler to enforce the
  cap via the `gpu_slot` custom resource instead — it holds cluster-wide by
  construction and there's no counter of mine to get wrong under failure.
- **LLM concurrency was silently capped by CPU.** On a small cluster the effective
  LLM concurrency sat below 8. The tasks mostly sleep on "network", so full CPU
  reservations were the real limiter — I set `num_cpus=0.1` on the summarise task so
  the window can actually reach the cap.
- **Retry inside the task, not Ray max_retries.** I kept the 429 backoff *inside*
  the task so a retrying ticket holds its in-flight slot (concurrency stays honest)
  and Ray never invisibly re-fires an API call. `max_retries=0` on the task so a
  lost worker surfaces as a failed ticket I can re-materialize, not a hidden retry.

## Verification

I didn't trust it until it ran. End-to-end at N=40 and N=200 locally, confirming:
retries occurred but final failures were 0, embed concurrency stayed pinned at 2 in
the Ray dashboard, no 429 storm on the LLM step, and every asset emitted metadata.
The design reasoning (bottleneck, 100x, micro-batch vs bulk, KubeRay mapping) is my
own and lives in `DECISIONS.md`.
