# Data Pipeline Exercise — Dagster + Ray (~2–3 hours)

## What this assignment is

Build and operate a small **data pipeline** that enriches support tickets, using
**Dagster** (orchestration) and **Ray** (distributed compute). The pipeline has a mix of
workloads — cheap & quick ELT, CPU-bound compute, scarce GPU inference, and a rate-limited LLM API
call — each with a **real resource constraint**.

> **No, you've not landed to a wrong assignment. This exercise is meant to take you outside your comfort zone.** It's fine if Dagster
> and Ray are new to you — that's the point. We want to see how you respond to something
> unfamiliar: how quickly you learn it, and how resourceful you are in picking up a new
> problem like this. Don't worry about knowing it all up front; show us how you figure it
> out.

You're evaluated on **infrastructure and operations judgement, not ML**. The models are
mocked; the constraints are real. Concretely, we want to see how you:

- **build the pipeline** — wire compute into Dagster as a clean, runnable job;
- **run it on Ray** — distribute the heavy work across a cluster *within its limits*;
- **scale it up and down** — across both **size and cadence**: small frequent runs (latency-/SLA-driven)
  vs large infrequent ones (throughput-/cost-driven), 200 vs 10,000 tickets, more/fewer workers, gracefully;
- **make it observable** — so anyone can see if it finished, how many tickets failed/retried, where the bottleneck is, and
  whether a limit is being hit, **without reading the code**;
- think about the **other infra concerns** that come with this (failure handling, backpressure,
  cost/scale-to-zero, what you'd alert on);
- **lean on automation** — we believe in heavy automation, so show where you'd remove manual
  toil: making this easy to run and maintain, quick to onboard a new integration/step, and
  fast to debug when something breaks.

Use Claude Code freely — we want to see how you use it.

## What's already done vs. what you do

**Done for you — the Dagster part.** A working Dagster project: the six pipeline steps are
defined as assets and the pipeline runs end-to-end **today**. But it runs entirely **in one
process, sequentially — there is no Ray and no cluster yet.**

**Your job — the Ray part + the infra around it.** Stand up a Ray cluster, run the heavy
steps on it within their constraints, and address the operational concerns above (scaling,
observability, robustness). Details below.

```
raw_tickets → cleaned → featurized → embedded → summarised → enriched
 (ingest)     (ELT)      (CPU) ⬅TODO  (GPU) ⬅TODO (LLM) ⬅TODO   (serve)
```

The assets live in [`pipeline/assets.py`](pipeline/assets.py); the code location in
[`pipeline/definitions.py`](pipeline/definitions.py); a place to add a Ray resource in
[`pipeline/resources.py`](pipeline/resources.py). The mock models and their limits are in
[`mocks.py`](mocks.py) / [`config.yaml`](config.yaml) (you won't edit those).

> The `ray` library **is already installed in the image** (Ray 2.55.1) — you don't need to
> add the dependency. You need to stand up a cluster and use it.

## Run it — the starting point

```bash
docker compose up -d --build
```

Open the **Dagster UI → http://localhost:3000**, go to the asset graph, and click
**Materialize all**.

**What you'll see today:** all six steps run **one after another, inside the single `app`
container** — nothing is parallel and nothing runs on Ray. At `EXERCISE_N=200` it finishes
in a minute or two; `summarised` is the slow one (the mock LLM sleeps ~0.7s per ticket, and
right now they're called one at a time). There is **no Ray dashboard yet** — there's no Ray
running. Turning this serial pipeline into a Ray-backed one is the exercise.

Run size is the `EXERCISE_N` env var (default 200). A big run later (no code change):
`EXERCISE_N=10000 docker compose up -d`.

## What you must do

**1. Stand up a Ray cluster in [`docker-compose.yml`](docker-compose.yml).** Add a head
and one or more workers (`ray start …`), publish the Ray dashboard (`:8265`), and make the
**GPU worker advertise the scarce `gpu_slot` resource** (`config.yaml` → `gpu_slots`). Make
the worker count easy to change (so you can scale-test).

**2. Wire Ray into the pipeline and run the three heavy steps on it** — `featurized`,
`embedded`, `summarised` — **ideally as one job** (e.g. a single Ray Data / Ray job that
chains the three stages). Add a Ray resource in [`resources.py`](resources.py) and register
it in [`definitions.py`](definitions.py). Leave the cheap steps (`raw_tickets`, `cleaned`,
`enriched`) in-process — don't put them on Ray.

**3. Respect the constraints** ([`config.yaml`](config.yaml) — these are the numbers; don't
weaken them). A "constraint" here just means **how many of a step's tasks may run at the
same moment** (its concurrency), plus how to react when a limit is hit. The whole point is to
see **how you implement and enforce each one on Ray:**

   - `featurized` (**CPU**) — CPU-bound work. Spread it across workers, but **group tickets
     into batches** instead of one Ray task per ticket (10k tiny tasks is mostly overhead).
   - `embedded` (**GPU**) — GPUs are scarce: **at most 2 embeds may run at once**, cluster-wide.
     You make that real by giving the GPU worker `gpu_slot: 2` and having each embed task
     request one slot; then **batch** so each slot processes many tickets (it's batch-of-1
     today).
   - `summarised` (**LLM API**) — the API rejects a call (HTTP **429**) when **more than 8
     are in flight at once**, and randomly fails ~3% of calls. Keep your concurrency at/under
     8, and **retry the failures with backoff**. One bad ticket must not fail the whole run.

**4. Make it scale up and down.** "Scaling" here means changing how many workers run, so more
(or fewer) tasks run at the same moment — locally that's `docker compose up --scale
ray-worker=N` (even on one laptop, more workers = more parallel tasks, up to your CPU cores).
`EXERCISE_N=200` and `EXERCISE_N=10000` must both work with no code change; adding workers
must actually increase throughput; and it must behave sanely when load or workers drop.

> **Think of the two run sizes as two real operating modes — design for both.** In production
> this pipeline serves two kinds of customers:
> - **micro-batch** (think `EXERCISE_N=200`): a few latency-sensitive customers, run **every
>   ~5 min**, with a hard **15-min SLA** on insights.
> - **bulk** (think `EXERCISE_N=10000`): large jobs run **infrequently** (e.g. once a day),
>   where **throughput and cost** matter more than latency.
>
> These pull in opposite directions and want **different cluster/scheduling config** — same
> pipeline code, different posture. A couple of tensions to reason about: micro-batch **can't
> afford to cold-start workers every 5 min** (the spin-up tax eats the SLA and you're paying
> to wait), so it wants a small **warm** footprint; bulk **can't justify keeping workers hot
> all day** for one run (expensive idle), so it wants to **scale from zero, burst, and
> collapse back**. How would you express each mode in **Ray** (worker groups,
> `minReplicas`/`maxReplicas`, idle timeout, scale-to-zero) and in **Dagster** (schedules,
> run config, run-queue priority/concurrency, freshness/SLA)? You don't have to implement both
> — but be ready to discuss it, and note your design in `DECISIONS.md`.

**5. Make it observable.** We must answer, **without reading your code**: did the run
finish, and how many tickets succeeded/failed per step? where's the bottleneck — are we
saturating the GPU slots / LLM cap? Use **Dagster asset metadata**
(`context.add_output_metadata(...)` — see the given assets) + the **Ray dashboard** + at
least one metric/log stream (throughput, retries, in-flight LLM calls, GPU-slot use).

## Where each step runs (target)

| Asset | Work | **Target** | Limit to respect |
|---|---|---|---|
| `raw_tickets` | ingest | in-process | — |
| `cleaned` | ELT | in-process | — |
| `featurized` | CPU | **Ray** | chunk — don't spawn 10k tiny tasks |
| `embedded` | GPU | **Ray**, `resources={"gpu_slot": 1}` + batch | only **2 `gpu_slot`s** cluster-wide |
| `summarised` | LLM API | **Ray**, bounded concurrency | **≤ 8 in flight** + ~3% 429s → retry w/ backoff |
| `enriched` | serve | in-process | — |

## Before the follow-up discussion — read about KubeRay

This exercise runs Ray locally in Docker. In production, Ray typically runs on Kubernetes
via **KubeRay**. **Please read up on KubeRay and how it autoscales** — worker groups with
`minReplicas`/`maxReplicas`, scale-to-zero, and how the **Ray autoscaler** cooperates with
the **Kubernetes cluster autoscaler** (the pending-pod handshake). In the interview we'll
discuss **how you'd take your local design to KubeRay** — pools, autoscaling the GPU vs CPU
work, and what changes vs. what stays the same. Nothing to implement; just be ready to talk
about it.

Also jot down **automation opportunities you'd pursue with more time** — things you spotted
but didn't get to implement (onboarding a new step, self-healing, CI checks, etc.). We'll
talk through those too.

## Hand back

- Your code (incl. the `docker-compose.yml` with your Ray cluster) + how to run it.
- **`DECISIONS.md`** (~1 page): where the bottleneck is and what moves it; how you'd go
  **100×** and what breaks first; the scale-up *and* scale-down story; how you'd configure
  the **micro-batch vs bulk** modes differently (Ray + Dagster) and why; what you'd
  dashboard / alert on in prod; a few lines on the KubeRay mapping.
- **`NOTES.md`**: how you used Claude Code (a few prompts; where you trusted vs. corrected
  it) — or just share the transcript.

## Acceptance check (what we'll do)

1. `docker compose up` → Materialize all in Dagster at `EXERCISE_N=200` → succeeds, and the
   metadata tells us per-step counts/failures.
2. `EXERCISE_N=10000` with extra workers → finishes in reasonable time, the Ray dashboard
   shows work spread across workers, `embedded` stays pinned at 2 concurrent, and
   `summarised` stays under the LLM cap (no 429 storm).
3. Read your `DECISIONS.md` and talk through the KubeRay mapping.

## Rules

- Don't weaken the mock constraints (GPU slots, LLM cap, failure rate).
- No real models, keys, or cloud — everything runs locally in Compose.
- A small pipeline that **demonstrably scales and is observable** beats a big one that
  doesn't. Prioritise accordingly.

## Files

```
pipeline/
  assets.py        the 6 assets — the 3 heavy ones are yours to put on Ray
  resources.py     empty — add your Ray resource here
  definitions.py   the Dagster code location (assets + job)
mocks.py           the 3 mock models + enforced constraints (don't edit)
data.py            synthetic tickets
config.yaml        the constraints (don't weaken)
docker-compose.yml just the app today — you add the Ray cluster
Dockerfile         image with Ray + Dagster already installed
```
