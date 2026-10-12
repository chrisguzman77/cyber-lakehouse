"""Simulate network logs arriving over time.

Splits the raw UNSW-NB15 CSVs into fixed-size chunk files and releases the next
N of them into <landing>/incoming/, which the bronze stream watches. Each chunk
is written to <landing>/_staging/ first and moved with os.replace, so the stream
never sees a half-written file. Pure Python (no Spark) so it runs on the host,
in Docker, or in a Databricks notebook against a Volume path.

Usage: uv run python -m src.utils.replay --files 3
"""

from __future__ import annotations

import argparse
import glob
import os
from collections.abc import Iterator
from pathlib import Path

from src.utils.io import LANDING_DIR, RAW_DIR


def _chunks(path: str, chunk_rows: int) -> Iterator[tuple[int, list[bytes]]]:
    """Yield (index, lines) chunks of a file, keeping line endings byte-exact."""
    lines: list[bytes] = []
    idx = 0
    with open(path, "rb") as f:
        for line in f:
            lines.append(line)
            if len(lines) == chunk_rows:
                yield idx, lines
                idx += 1
                lines = []
    if lines:
        yield idx, lines


def release_chunks(
    source_glob: str, landing_dir: str, chunk_rows: int = 250_000, n_files: int = 3
) -> list[str]:
    """Release the next n_files chunks not yet in incoming/ (0 = all). Returns
    the released file names in release order."""
    incoming = Path(landing_dir) / "incoming"
    staging = Path(landing_dir) / "_staging"
    incoming.mkdir(parents=True, exist_ok=True)
    staging.mkdir(parents=True, exist_ok=True)
    already = set(os.listdir(incoming))

    released: list[str] = []
    for src in sorted(glob.glob(source_glob)):
        stem = Path(src).stem
        for idx, lines in _chunks(src, chunk_rows):
            name = f"{stem}.part-{idx:04d}.csv"
            if name in already:
                continue
            tmp = staging / name
            with open(tmp, "wb") as out:
                out.writelines(lines)
            os.replace(tmp, incoming / name)
            released.append(name)
            if len(released) == n_files:
                return released
    return released


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source_glob", default=str(RAW_DIR / "UNSW-NB15_*.csv"))
    p.add_argument("--landing_dir", default=str(LANDING_DIR))
    p.add_argument("--chunk_rows", type=int, default=250_000)
    p.add_argument("--files", type=int, default=3, help="Chunks to release (0 = all)")
    args = p.parse_args()

    released = release_chunks(args.source_glob, args.landing_dir, args.chunk_rows, args.files)
    print(f"[OK] Released {len(released)} chunk(s): {released}")


if __name__ == "__main__":
    main()
