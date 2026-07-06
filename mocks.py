"""The three mock "models". Bodies are fake; their CONSTRAINTS are real.

  cpu_featurize(text) -> dict   CPU-bound (busy-loop), scales with worker cores
  gpu_embed(texts)    -> list   needs a scarce `gpu_slot` Ray resource; batch it
  llm_summarise(text) -> dict   external API: 429s past 8 concurrent + ~3% random

You shouldn't need to change this file. Swap the bodies for real calls and the
constraints (and your infra around them) stay the same.
"""
from __future__ import annotations
import random, threading, time
from pathlib import Path
import yaml

CFG = yaml.safe_load((Path(__file__).parent / "config.yaml").read_text())


class RateLimitError(RuntimeError):
    """The mock LLM API's HTTP 429."""


def cpu_featurize(text: str) -> dict:
    end = time.perf_counter() + CFG["cpu_ms_per_ticket"] / 1000.0
    while time.perf_counter() < end:
        pass  # genuinely burn a core
    return {"tokens": max(1, len(text.split()))}


def gpu_embed(texts: list[str]) -> list[dict]:
    # Latency scales with batch size, so batching amortises it. Scarcity is
    # enforced by Ray: run this on a task that requests {"gpu_slot": 1}.
    time.sleep(CFG["gpu_ms_per_ticket"] / 1000.0 * len(texts))
    return [{"embedding_dim": 384} for _ in texts]


def llm_summarise(text: str) -> dict:
    cap = CFG["llm_max_in_flight"]
    lim = _limiter()
    n = lim.enter()
    try:
        if n > cap:
            raise RateLimitError(f"429: {n} in flight > cap {cap}")
        if random.random() < CFG["llm_error_rate"]:
            raise RateLimitError("429: transient (mock)")
        time.sleep(CFG["llm_latency_ms"] / 1000.0)
        return {"summary": text[:80], "intent": "complaint", "sentiment": "negative"}
    finally:
        lim.exit()


# ── shared in-flight counter so the LLM cap holds across Ray workers ──────────
# Backed by a Ray named actor when Ray is running; a local counter otherwise.
_lim = None
_lim_lock = threading.Lock()


def _limiter():
    global _lim
    if _lim is None:
        with _lim_lock:
            if _lim is None:
                _lim = _ray_limiter() or _LocalLimiter()
    return _lim


def _ray_limiter():
    try:
        import ray
        if not ray.is_initialized():
            return None
    except Exception:
        return None

    @ray.remote(num_cpus=0)
    class _C:
        def __init__(self): self.n = 0
        def enter(self): self.n += 1; return self.n
        def exit(self): self.n -= 1

    actor = _C.options(name="llm_limiter", get_if_exists=True, lifetime="detached").remote()

    class _W:
        def enter(self): return ray.get(actor.enter.remote())
        def exit(self): actor.exit.remote()
    return _W()


class _LocalLimiter:
    def __init__(self): self.n = 0; self.lock = threading.Lock()
    def enter(self):
        with self.lock: self.n += 1; return self.n
    def exit(self):
        with self.lock: self.n -= 1
