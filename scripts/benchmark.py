"""Latency/throughput benchmark for the /score endpoint.

Fires `--n` requests at `--url` using a thread pool of `--concurrency`
workers. Events are sampled (with replacement, deterministic seed) from
`data/sample/transactions_sample.csv` via `src.pipeline.ingestion.to_event`
so the traffic looks like real production payloads. Reports wall-clock
p50/p95/p99 latency (ms), mean latency, throughput (req/s), and error count
as a markdown table (paste-ready for the README) and JSON at
`benchmarks/latest.json`.

Usage:
    .venv/bin/python scripts/benchmark.py --n 2000 --concurrency 8
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

from src.pipeline.ingestion import TOKENIZATION_SALT, to_event  # noqa: E402

DEFAULT_URL = "http://localhost:8000/score"
DEFAULT_CSV = os.path.join(_REPO_ROOT, "data", "sample", "transactions_sample.csv")
DEFAULT_OUTPUT = os.path.join(_REPO_ROOT, "benchmarks", "latest.json")


def _load_events(csv_path: str, n: int, seed: int) -> list[dict[str, Any]]:
    """Build n contract-v1 events sampled (with replacement) from the sample CSV."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    rng = random.Random(seed)
    events: list[dict[str, Any]] = []
    for _ in range(n):
        row = rng.choice(rows)
        try:
            event = to_event(row, TOKENIZATION_SALT)
        except (KeyError, ValueError):
            continue
        event.pop("is_fraud", None)
        events.append(event)
    # Backfill in case some rows failed to map, so we still return n events.
    while len(events) < n:
        row = rng.choice(rows)
        try:
            event = to_event(row, TOKENIZATION_SALT)
        except (KeyError, ValueError):
            continue
        event.pop("is_fraud", None)
        events.append(event)
    return events


def _fire_one(url: str, event: dict[str, Any]) -> tuple[float, int, bool]:
    """POST one event; returns (latency_ms, status_code, is_error)."""
    start = time.perf_counter()
    try:
        resp = requests.post(url, json=event, timeout=10)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return elapsed_ms, resp.status_code, resp.status_code >= 400
    except requests.RequestException:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return elapsed_ms, 0, True


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    if lo == hi:
        return sorted_values[lo]
    frac = k - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


def run_benchmark(url: str, n: int, concurrency: int, csv_path: str, seed: int = 42) -> dict[str, Any]:
    events = _load_events(csv_path, n, seed)

    latencies: list[float] = []
    errors = 0

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_fire_one, url, event) for event in events]
        for future in as_completed(futures):
            latency_ms, _status, is_error = future.result()
            latencies.append(latency_ms)
            if is_error:
                errors += 1
    wall_elapsed = time.perf_counter() - wall_start

    latencies.sort()
    throughput = n / wall_elapsed if wall_elapsed > 0 else 0.0

    return {
        "n": n,
        "concurrency": concurrency,
        "url": url,
        "wall_seconds": wall_elapsed,
        "throughput_rps": throughput,
        "errors": errors,
        "p50_ms": _percentile(latencies, 50),
        "p95_ms": _percentile(latencies, 95),
        "p99_ms": _percentile(latencies, 99),
        "mean_ms": sum(latencies) / len(latencies) if latencies else 0.0,
    }


def _to_markdown(result: dict[str, Any]) -> str:
    return (
        "| n | concurrency | throughput (req/s) | mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) | errors |\n"
        "|---|---|---|---|---|---|---|---|\n"
        f"| {result['n']} | {result['concurrency']} | {result['throughput_rps']:.1f} "
        f"| {result['mean_ms']:.2f} | {result['p50_ms']:.2f} | {result['p95_ms']:.2f} "
        f"| {result['p99_ms']:.2f} | {result['errors']} |"
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Benchmark the /score endpoint.")
    parser.add_argument("--n", type=int, default=2000, help="Total number of requests to fire.")
    parser.add_argument("--concurrency", type=int, default=8, help="Thread pool size.")
    parser.add_argument("--url", default=DEFAULT_URL, help="Target /score URL.")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Sample CSV to draw events from.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Path to write JSON results.")
    parser.add_argument("--seed", type=int, default=42, help="Sampling seed (reproducible traffic mix).")
    args = parser.parse_args(argv)

    result = run_benchmark(args.url, args.n, args.concurrency, args.csv, args.seed)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(_to_markdown(result))
    print(f"\nWrote {args.output}")


if __name__ == "__main__":
    main(sys.argv[1:])
