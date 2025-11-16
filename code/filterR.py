"""Filter World Values Survey columns whose IDs end with the "R" suffix.

The raw extract `Q1_Q260_filtered_30%row_and_column.csv` contains recoded
columns (suffix `R`) that we do not need for the downstream analysis.
This script removes those columns and writes a clean copy alongside the source
file without modifying the original dataset.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "Data"
SOURCE_FILE = DATA_DIR / "Q1_Q260_filtered_30%row_and_column.csv"
OUTPUT_FILE = DATA_DIR / "Q1_Q260_filtered_30%row_and_column_no_R.csv"
CHUNK_SIZE = 50_000


def _columns_to_drop(columns: Iterable[str], suffix: str = "R") -> list[str]:
	"""Return the list of columns that end with the provided suffix."""

	return [column for column in columns if column.endswith(suffix)]


def _write_filtered_csv(columns_to_drop: list[str]) -> None:
	"""Stream the source CSV and write a filtered copy without the target columns."""

	is_first_chunk = True
	for chunk in pd.read_csv(SOURCE_FILE, chunksize=CHUNK_SIZE):
		filtered_chunk = chunk.drop(columns=columns_to_drop, errors="ignore")
		filtered_chunk.to_csv(
			OUTPUT_FILE,
			mode="w" if is_first_chunk else "a",
			header=is_first_chunk,
			index=False,
		)
		is_first_chunk = False


def main() -> int:
	if not SOURCE_FILE.exists():
		msg = (
			"Source file not found. Expected input at "
			f"{SOURCE_FILE}\n"
			"Update Code/filter.py if the dataset has moved."
		)
		print(msg, file=sys.stderr)
		return 1

	print(f"Loading column headers from {SOURCE_FILE.name}...")
	first_chunk = next(pd.read_csv(SOURCE_FILE, chunksize=CHUNK_SIZE))
	columns_to_drop = _columns_to_drop(first_chunk.columns)

	if not columns_to_drop:
		print("No columns ending with 'R' found; nothing to filter.")
		return 0

	print(
		"Dropping columns (sample): "
		+ ", ".join(columns_to_drop[:5])
		+ ("..." if len(columns_to_drop) > 5 else "")
	)

	_write_filtered_csv(columns_to_drop)

	print(
		f"Filtered dataset written to {OUTPUT_FILE.name} with "
		f"{len(columns_to_drop)} columns removed."
	)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
