# DECISIONS

## How to run

```bash
docker compose up -d --build                                # Dagster :3000, Ray dashboard :8265
# Materialize all in the Dagster UI
docker compose up -d --scale ray-worker=4                   # more CPU workers, no code change
EXERCISE_N=10000 docker compose up -d --scale ray-worker=4  # bulk run
```

## Architecture

Head node (`--num-cpus=0`, so nothing schedules on it), N generic CPU workers
(`--scale ray-worker=N`), and one "GPU" worker that alone advertises the custom
resource `gpu_slot: 2`. Dagster reaches the cluster over the Ray client
(`ray://ray-head:10001`) — I use plain Ray core tasks, not Ray Data, so the driver
doesn't need to be a cluster node; if I moved to Ray Data streaming I'd run the
Dagster step as a Ray job on-cluster instead. All containers share one image and
the `/app` bind-mount, so task code imports by reference with no runtime_env upload.

## Enforcing the constraints

- **featurized (CPU):** chunked into batches of 100 (`FEATURIZE_BATCH`) — ~100 tasks
  at N=10k, not 10k tiny ones. Throughput scales with worker count.
- **embedded (GPU):** batches of 64, each task requests `{"gpu_slot": 1}`. Only 2
  slots exist cluster-wide, so the **Ray scheduler itself** enforces ≤2 concurrent;
  queued tasks are natural backpressure. No semaphores in my code to get wrong.
- **summarised (LLM):** sliding window via `ray.wait()` keeps ≤8 tasks in flight;
  each task retries transient 429s **inside the task** (exponential backoff +
  jitter), so a retrying ticket still holds its window slot and the cluster-wide
  in-flight count can't exceed the cap. `num_cpus=0.1` because these tasks sleep on
  I/O — CPU reservations shouldn't starve the window. After max attempts a ticket is
  recorded as failed; one bad ticket never fails the run. Task-level `max_retries=0`
  so Ray never silently re-fires an API call; a lost worker surfaces as a failed
  ticket and the asset can be re-materialized.

## Bottleneck and what moves it

The LLM cap dominates: N × 0.7s / 8 → ~18s at N=200, **~15 min at N=10k** (everything
else is seconds). Moving it means raising the cap (vendor tier / more keys), batching
multiple tickets per call, or dedupe/caching of near-identical tickets — not more
workers. Featurize scales linearly with workers; embed is pinned at 2 slots by design.

## 100× (1M tickets) — what breaks first

1. **Materialized lists in memory** — Dagster passes full Python lists between assets
   and pickles them through the IO manager. First fix: hand off Parquet (or Ray
   object refs) between steps, or fuse the three heavy steps into one streaming Ray
   Data job (`map_batches` with `concurrency=` and `resources=` per stage).
2. **Driver-side per-ticket submission** for the LLM step → move to a Ray actor pool
   or Ray Data with `concurrency=8`.
3. Even fixed, the LLM cap makes 1M ≈ 24h — that's a quota conversation, not an
   infra fix; shard across keys/regions or negotiate batch endpoints.

## Scale up / down

Up: `--scale ray-worker=N` — featurize throughput rises with cores; embed/LLM stay
pinned at their caps (verified in the Ray dashboard). Down: batch tasks are pure
functions, Ray reschedules them if a worker dies (default task retries); the LLM
step degrades to recorded failures rather than duplicate calls. Fewer workers than
tasks just means a longer queue, never a crash.

## Micro-batch vs bulk (same code, different posture)

- **Micro-batch (N=200, every 5 min, 15-min SLA):** a small **warm** pool — KubeRay
  cpu worker group `minReplicas: 2`, long `idleTimeoutSeconds`, so no cold-start tax
  inside the SLA. Dagster: the included 5-min schedule, high run-queue priority,
  freshness checks/SLA alert on `enriched`.
- **Bulk (N=10k, daily):** **scale-from-zero, burst, collapse** — `minReplicas: 0`,
  high `maxReplicas`, short idle timeout; bigger `FEATURIZE_BATCH`/`EMBED_BATCH`;
  Dagster off-peak schedule, low priority, run-level concurrency of 1 so bulk never
  starves micro-batch. Cold-start is amortized over a long run, idle cost ~0.

## Observability

Without reading code: Dagster asset metadata shows per-step counts, failures,
retries, batch sizes, seconds, throughput, and **effective LLM concurrency vs the
cap** (are we saturating the 8?); the Ray dashboard shows tasks spread across
workers and `gpu_slot` usage pinned at 2; structured step logs show progress every
100 tickets. In prod: Ray's Prometheus metrics + Grafana. Alert on: run
failure/SLA miss, LLM 429/retry rate spike, effective concurrency ≪ cap
(something upstream is starving it), GPU-slot queue depth growing, worker
restarts/OOM, object-store spill.

## KubeRay mapping

Compose services → `RayCluster` CRD: head pod (+`enableInTreeAutoscaling`), two
`workerGroupSpecs` — `cpu-pool` (min/max replicas per mode above) and `gpu-pool`
with `rayStartParams.resources: '{"gpu_slot": 2}'` (real GPUs: `nvidia.com/gpu`
requests instead). Scaling handshake: Ray autoscaler sees pending tasks → asks for
worker pods → pods Pending → **K8s cluster autoscaler** provisions nodes → pods
schedule → tasks run; scale-down reverses via idle timeout. Dagster runs alongside
with the k8s run launcher; per-mode run config selects batch sizes and priorities.

## Automation backlog (with more time)

CI smoke test (`compose up`, materialize at N=50, assert metadata counts/0 unexpected
failures); a scaffold generator for onboarding a new pipeline step (asset + remote fn
+ metadata boilerplate); Dagster RetryPolicy + a sensor that re-materializes only
failed tickets; batch sizes auto-tuned from last run's metadata; alert rules and
dashboards as code.
