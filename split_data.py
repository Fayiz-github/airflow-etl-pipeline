"""
split_data.py
─────────────
Splits the master creditcard.csv into equal-sized chunk files
(task_1.csv … task_N.csv) inside data/source/.

Usage:
    python split_data.py                  # default: 6 chunks
    python split_data.py --chunks 10      # custom number of chunks

The extract pipeline reads these chunk files as rotating source data.
"""

import argparse
import math
import os

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.join(BASE_DIR, "data", "source")
INPUT_FILE = os.path.join(SOURCE_DIR, "creditcard.csv")


def split(num_chunks: int = 6) -> None:
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(
            f"Source file not found: {INPUT_FILE}\n"
            "Place creditcard.csv inside data/source/ before running."
        )

    print(f"Reading {INPUT_FILE} …")
    df = pd.read_csv(INPUT_FILE)
    total_rows = len(df)
    chunk_size = math.ceil(total_rows / num_chunks)

    print(f"Total rows : {total_rows:,}")
    print(f"Chunks     : {num_chunks}")
    print(f"Chunk size : ~{chunk_size:,} rows each")
    print()

    for i in range(num_chunks):
        start  = i * chunk_size
        end    = min(start + chunk_size, total_rows)
        chunk  = df.iloc[start:end]
        out    = os.path.join(SOURCE_DIR, f"task_{i + 1}.csv")
        chunk.to_csv(out, index=False)
        print(f"  ✅ task_{i + 1}.csv  →  {len(chunk):,} rows  →  {out}")

    print(f"\nDone. {num_chunks} chunk files written to {SOURCE_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split creditcard.csv into task chunks.")
    parser.add_argument(
        "--chunks", type=int, default=6,
        help="Number of output chunk files (default: 6)"
    )
    args = parser.parse_args()
    split(args.chunks)
