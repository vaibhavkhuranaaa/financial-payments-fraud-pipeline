"""CLI entry point for the full-data fraud workbench build."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.fraud_workbench.artifacts import build_run
from src.fraud_workbench.modeling import TrainingConfig

REPO_ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data", type=Path, default=REPO_ROOT / "data" / "raw" / "creditcard.csv"
    )
    parser.add_argument("--artifacts", type=Path, default=REPO_ROOT / "artifacts")
    parser.add_argument("--reviews-per-1000", type=float, default=1.0)
    parser.add_argument("--bootstrap-samples", type=int, default=300)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = TrainingConfig(
        reviews_per_1000=args.reviews_per_1000,
        bootstrap_samples=args.bootstrap_samples,
    )
    result = build_run(args.data, args.artifacts, config=config, force=args.force)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
