"""Dagster resources — the Ray cluster connection.

The Dagster process talks to the cluster over the Ray client
(`ray://ray-head:10001`). We use plain Ray core tasks (not Ray Data), so the
driver does NOT need to be a cluster node — the client is enough, and it keeps
the Dagster container out of the scheduling pool. Code is importable on every
node because the same image + /app bind-mount is shared by app/head/workers
(no runtime_env upload needed).
"""
from __future__ import annotations

import os

import dagster as dg


class RayResource(dg.ConfigurableResource):
    """Hands assets an initialised `ray` module connected to the cluster."""

    address: str = os.environ.get("RAY_ADDRESS", "ray://ray-head:10001")

    def get_ray(self):
        import ray

        if not ray.is_initialized():
            ray.init(address=self.address, ignore_reinit_error=True)
        return ray
