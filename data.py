"""Synthetic support tickets — the pipeline's input. In-memory, no database.

  make_tickets(2000) -> [{"ticket_id", "client_id", "body"}, ...]
"""
from __future__ import annotations
import random

_CLIENTS = ["acme", "globex", "initech", "umbrella", "hooli"]
_FRAGMENTS = [
    "my order is late again", "the app keeps crashing on checkout",
    "i was charged twice this month", "cannot reset my password",
    "the delivery arrived damaged", "need a refund for the duplicate charge",
    "the new update broke my dashboard", "shipment stuck in transit for a week",
]


def make_tickets(n: int) -> list[dict]:
    out = []
    for i in range(n):
        body = ". ".join(random.choice(_FRAGMENTS) for _ in range(random.randint(1, 5)))
        out.append({"ticket_id": f"T-{i:07d}",
                    "client_id": random.choice(_CLIENTS),
                    "body": "  " + body.capitalize() + ".  "})
    return out
