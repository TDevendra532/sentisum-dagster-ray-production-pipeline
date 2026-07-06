"""The pipeline as Dagster assets.

    raw_tickets → cleaned → featurized → embedded → summarised → enriched
    (ingest)      (ELT)      (CPU/Ray)   (GPU/Ray)   (LLM/Ray)    (serve)

The three heavy steps now run on the Ray cluster (see docker-compose.yml),
each within its constraint from config.yaml:

  featurized  CPU-bound  → chunked into batches (FEATURIZE_BATCH, default 100)
                           so 10k tickets ≠ 10k tiny tasks; spreads across
                           all CPU workers.
  embedded    GPU        → batched (EMBED_BATCH, default 64); each batch task
                           requests {"gpu_slot": 1}. Only the GPU worker
                           advertises gpu_slot=2, so at most 2 embed batches
                           run at once, cluster-wide — enforced by the Ray
                           scheduler, not by hope.
  summarised  LLM API    → a sliding window of at most `llm_max_in_flight`
                           (8) tasks in flight, driven by ray.wait(); each
                           task retries transient 429s with exponential
                           backoff + jitter. One bad ticket never fails the
                           run — after max attempts it's recorded as failed.

The cheap steps (raw_tickets, cleaned, enriched) stay in-process.
Run size is `EXERCISE_N` (env): N=200 and N=10000 need no code change.
"""
import os
import time

import dagster as dg
from dagster import AssetExecutionContext

from data import make_tickets
from mocks import CFG
from pipeline.resources import RayResource


def _n() -> int:
    return int(os.environ.get("EXERCISE_N", "200"))


def _chunks(rows: list, size: int) -> list[list]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]


def _throughput_meta(n: int, seconds: float) -> dict:
    return {
        "seconds": round(seconds, 2),
        "throughput_tickets_per_s": round(n / seconds, 1) if seconds else 0,
    }


# ── remote task bodies (imported by reference on workers via the shared /app) ─

def _featurize_batch(rows: list[dict]) -> list[dict]:
    from mocks import cpu_featurize

    for r in rows:
        r["features"] = cpu_featurize(r["text"])
    return rows


def _embed_batch(rows: list[dict]) -> list[dict]:
    from mocks import gpu_embed

    for r, emb in zip(rows, gpu_embed([r["text"] for r in rows])):
        r["embedding"] = emb
    return rows


def _summarise_one(row: dict, max_attempts: int = 5) -> dict:
    """Summarise one ticket, retrying transient 429s with backoff + jitter.

    Retries happen INSIDE the task, so the ticket keeps its window slot and
    the cluster-wide in-flight count never exceeds the cap.
    """
    import random as _random
    import time as _time

    from mocks import RateLimitError, llm_summarise

    for attempt in range(max_attempts):
        try:
            row["summary"] = llm_summarise(row["text"])
            row["_retries"] = attempt
            return row
        except RateLimitError:
            _time.sleep(min(0.25 * (2**attempt), 4.0) + _random.random() * 0.25)
    row["summary"] = {"error": "rate limited"}
    row["_retries"] = max_attempts - 1
    return row


# ── given: ingest (stays in-process) ─────────────────────────────────────────
@dg.asset
def raw_tickets(context: AssetExecutionContext) -> list[dict]:
    tickets = make_tickets(_n())
    context.add_output_metadata({"count": len(tickets)})
    return tickets


# ── given: ELT (stays in-process) ────────────────────────────────────────────
@dg.asset
def cleaned(context: AssetExecutionContext, raw_tickets: list[dict]) -> list[dict]:
    rows = [{**t, "text": " ".join(t["body"].split())} for t in raw_tickets]
    context.add_output_metadata({"count": len(rows)})
    return rows


# ── CPU compute on Ray: chunked so task overhead stays negligible ────────────
@dg.asset
def featurized(
    context: AssetExecutionContext, cleaned: list[dict], ray_cluster: RayResource
) -> list[dict]:
    ray = ray_cluster.get_ray()
    batch = int(os.environ.get("FEATURIZE_BATCH", "100"))
    task = ray.remote(_featurize_batch)

    t0 = time.perf_counter()
    refs = [task.remote(c) for c in _chunks(cleaned, batch)]
    rows = [r for out in ray.get(refs) for r in out]
    dt = time.perf_counter() - t0

    context.log.info(f"featurized {len(rows)} tickets in {dt:.1f}s across {len(refs)} Ray tasks")
    context.add_output_metadata(
        {"count": len(rows), "ray_tasks": len(refs), "batch_size": batch,
         **_throughput_meta(len(rows), dt)}
    )
    return rows


# ── GPU inference on Ray: batched, gated by the scarce gpu_slot resource ─────
@dg.asset
def embedded(
    context: AssetExecutionContext, featurized: list[dict], ray_cluster: RayResource
) -> list[dict]:
    ray = ray_cluster.get_ray()
    batch = int(os.environ.get("EMBED_BATCH", "64"))
    # Each batch task holds one gpu_slot; only gpu_slots (2) exist cluster-wide,
    # so the Ray scheduler itself caps concurrency at 2 — the queue is backpressure.
    task = ray.remote(_embed_batch).options(resources={"gpu_slot": 1})

    t0 = time.perf_counter()
    refs = [task.remote(c) for c in _chunks(featurized, batch)]
    rows = [r for out in ray.get(refs) for r in out]
    dt = time.perf_counter() - t0

    context.log.info(
        f"embedded {len(rows)} tickets in {dt:.1f}s "
        f"({len(refs)} batches × {batch}, ≤{CFG['gpu_slots']} concurrent gpu_slots)"
    )
    context.add_output_metadata(
        {"count": len(rows), "ray_tasks": len(refs), "batch_size": batch,
         "gpu_slots_cluster_wide": CFG["gpu_slots"], **_throughput_meta(len(rows), dt)}
    )
    return rows


# ── LLM calls on Ray: sliding window ≤ cap, retry w/ backoff inside the task ─
@dg.asset
def summarised(
    context: AssetExecutionContext, embedded: list[dict], ray_cluster: RayResource
) -> list[dict]:
    ray = ray_cluster.get_ray()
    cap = int(CFG["llm_max_in_flight"])
    # num_cpus=0.1: these tasks mostly sleep on "network"; don't let CPU
    # reservations (not real usage) starve the window below the cap.
    task = ray.remote(_summarise_one).options(num_cpus=0.1, max_retries=0)

    t0 = time.perf_counter()
    results: list[dict] = []
    in_flight: list = []
    for row in embedded:
        if len(in_flight) >= cap:  # window full → wait for one to finish
            done, in_flight = ray.wait(in_flight, num_returns=1)
            results.extend(ray.get(done))
            if len(results) % 100 < 1:
                context.log.info(f"summarised {len(results)}/{len(embedded)}…")
        in_flight.append(task.remote(row))
    results.extend(ray.get(in_flight))
    dt = time.perf_counter() - t0

    failed = sum(1 for r in results if "error" in r.get("summary", {}))
    retries = sum(r.pop("_retries", 0) for r in results)
    eff = (len(results) * CFG["llm_latency_ms"] / 1000.0) / dt if dt else 0

    context.log.info(
        f"summarised {len(results)} tickets in {dt:.1f}s | "
        f"failed={failed} retries={retries} effective_concurrency≈{eff:.1f}/{cap}"
    )
    context.add_output_metadata(
        {"count": len(results), "failed": failed, "retries": retries,
         "in_flight_cap": cap, "effective_concurrency": round(eff, 1),
         **_throughput_meta(len(results), dt)}
    )
    return results


# ── given: serve (stays in-process) ──────────────────────────────────────────
@dg.asset
def enriched(context: AssetExecutionContext, summarised: list[dict]) -> None:
    failed = sum(1 for r in summarised if "error" in r.get("summary", {}))
    context.add_output_metadata(
        {"tickets": len(summarised), "enriched": len(summarised) - failed, "failed": failed}
    )
