"""Download the IBM TabFormer credit-card dataset and carve a committed local sample.

Full dataset (~24M rows, ~2.3GB extracted) goes to data/raw/ (git-ignored).
The sample (~100k rows) is carved at the *user* level so per-card transaction
sequences stay intact — required for realistic windowed features — and is
biased to include every sampled user's full history plus a guaranteed set of
fraud-affected users. Seeded for reproducibility.

Usage:
    python scripts/get_data.py            # download + extract full dataset
    python scripts/get_data.py --sample   # carve data/sample/transactions_sample.csv
    python scripts/get_data.py --all      # both
"""

from __future__ import annotations

import argparse
import random
import sys
import tarfile
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
SAMPLE_PATH = REPO_ROOT / "data" / "sample" / "transactions_sample.csv"
ARCHIVE_PATH = RAW_DIR / "transactions.tgz"
CSV_NAME = "card_transaction.v1.csv"

# TabFormer stores the archive in Git LFS; this is the LFS media URL.
DATA_URL = (
    "https://media.githubusercontent.com/media/IBM/TabFormer/main/"
    "data/credit_card/transactions.tgz"
)

SEED = 42
TARGET_SAMPLE_ROWS = 100_000
FRAUD_USERS = 60      # guarantee fraud signal in the sample
RANDOM_USERS = 40     # plus typical users without fraud
CHUNK_ROWS = 1_000_000
# TabFormer users each have ~12k txns over decades — keep only each user's most
# recent contiguous history so the sample stays commit-sized while preserving
# per-card sequences for windowed features.
ROWS_PER_USER = TARGET_SAMPLE_ROWS // (FRAUD_USERS + RANDOM_USERS)


def download() -> Path:
    import requests

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RAW_DIR / CSV_NAME
    if csv_path.exists():
        print(f"already extracted: {csv_path}")
        return csv_path

    if not ARCHIVE_PATH.exists():
        print(f"downloading {DATA_URL} -> {ARCHIVE_PATH} (~2GB, be patient)")
        with requests.get(DATA_URL, stream=True, timeout=60) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done = 0
            with open(ARCHIVE_PATH, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
                    done += len(chunk)
                    if total:
                        print(f"\r  {done / 1e6:,.0f} / {total / 1e6:,.0f} MB", end="")
        print()

    print(f"extracting {ARCHIVE_PATH}")
    with tarfile.open(ARCHIVE_PATH) as tar:
        tar.extractall(RAW_DIR, filter="data")
    # archive may nest the csv; normalize location
    for p in RAW_DIR.rglob(CSV_NAME):
        if p != csv_path:
            p.rename(csv_path)
    if not csv_path.exists():
        sys.exit(f"extraction finished but {CSV_NAME} not found under {RAW_DIR}")
    print(f"extracted: {csv_path}")
    return csv_path


def carve_sample(csv_path: Path) -> None:
    rng = random.Random(SEED)

    # Pass 1: per-user row counts and fraud flags (only 2 columns -> fast, low memory)
    print("pass 1/2: scanning users and fraud distribution")
    counts: dict[int, int] = {}
    fraud_users: set[int] = set()
    for chunk in pd.read_csv(
        csv_path, usecols=["User", "Is Fraud?"], chunksize=CHUNK_ROWS
    ):
        for user, n in chunk["User"].value_counts().items():
            counts[user] = counts.get(user, 0) + int(n)
        fraud_users.update(chunk.loc[chunk["Is Fraud?"] == "Yes", "User"].unique())

    print(f"  {len(counts):,} users, {len(fraud_users):,} fraud-affected users")

    # Choose users: guaranteed fraud-affected users plus typical users
    chosen = set(rng.sample(sorted(fraud_users), min(FRAUD_USERS, len(fraud_users))))
    non_fraud = [u for u in sorted(counts) if u not in fraud_users]
    chosen.update(rng.sample(non_fraud, min(RANDOM_USERS, len(non_fraud))))
    print(f"  selected {len(chosen):,} users, most recent {ROWS_PER_USER:,} txns each")

    # Pass 2: collect chosen users' rows, keep each user's most recent contiguous slice
    print("pass 2/2: extracting selected users")
    parts = []
    for chunk in pd.read_csv(csv_path, chunksize=CHUNK_ROWS, dtype=str):
        part = chunk[chunk["User"].astype(int).isin(chosen)]
        if len(part):
            parts.append(part)
    df = pd.concat(parts, ignore_index=True)
    # source file is already in per-user chronological order; keep each tail
    df = df.groupby("User", sort=False, group_keys=False).tail(ROWS_PER_USER)
    SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(SAMPLE_PATH, index=False)

    frauds = int(df["Is Fraud?"].eq("Yes").sum())
    print(f"wrote {SAMPLE_PATH}: {len(df):,} rows, {frauds:,} fraud ({frauds / len(df):.3%})")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", action="store_true", help="carve the committed sample")
    ap.add_argument("--all", action="store_true", help="download and carve sample")
    args = ap.parse_args()

    csv_path = RAW_DIR / CSV_NAME
    if args.sample and not args.all:
        if not csv_path.exists():
            sys.exit("full dataset missing — run without --sample first (or use --all)")
    else:
        csv_path = download()
    if args.sample or args.all:
        carve_sample(csv_path)


if __name__ == "__main__":
    main()
