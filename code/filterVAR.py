"""Subset World Values Survey features into autonomy, competence, and relatedness.

This script keeps the target life satisfaction item (WVS Q49) and groups of
predictor variables tagged for SDT constructs. Populate the column mappings
below with the appropriate WVS column IDs before running the script.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Iterator, Tuple

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

# Process the derivative file that already stripped reverse-coded columns.
DEFAULT_SOURCE = DATA_DIR / "Q1_Q260_filtered_30%row_and_column_no_R.csv"
FALLBACK_SOURCE = DATA_DIR / "Q1_Q260_filtered_30%row_and_column.csv"
OUTPUT_FILE = DATA_DIR / "lifesat_sdt_subset.csv"

TARGET_COLUMN = "Q49"
TARGET_RENAMED = "LifeSat"

# Each mapping is ordered: keys are WVS column IDs, values are optional rename targets.
COLUMN_GROUPS: dict[str, dict[str, str]] = {
	"com": {
		"Q47": "SHealth",
		"Q50": "FinSat",
		"Q56": "FinSat_ComParent",
		"Q142": "RiskUnemployed",
		"Q143": "EducationNextGen",
		# "Q275": "Education",
		# "Q279": "PaidEmployment",
		# "Q281": "OccupationalGroup",
		# "Q285": "ChiefWageEarner",
		# "Q286": "FamFin",
		# "Q287": "SocialStatus",
		# "Q288": "HouseholdSocialStatus",
	},
	"aut": {
		"Q48": "FreeChoice",
		"Q131": "Security",
		"Q146": "PublicSecurity_War",
		"Q147": "PublicSecurity_Terrorism",
        "Q148": "PublicSecurity_CivilWar",
		"Q251": "Democracy",
		"Q253": "HumanRights",
	},
	"rel": {
		"Q57": "Trust",
		"Q58": "Trust_Family",
		"Q59": "Trust_Neighbors",
        "Q60": "Trust_Acquaintances",
		"Q61": "Trust_Strangers",
		"Q62": "Trust_OtherReligion",
		"Q63": "Trust_OtherNationality",
		"Q94": "Membership_Religious",
		"Q95": "Membership_Sport",
		"Q96": "Membership_Art",
		"Q97": "Membership_LaborUnion",
		"Q98": "Membership_Political",
		"Q99": "Membership_Environmental",
		"Q100": "Membership_Professional",
		"Q101": "Membership_Charity",
        "Q102": "Membership_Consumer",
		"Q103": "Membership_SelfHelp",
		"Q104": "Membership_Women",
		"Q105": "Membership_Other",
		"Q164": "GodImportance",
		"Q171": "ReligiousAttendance",
		"Q172": "Pray",
		"Q254": "NationalPride",
		"Q255": "CloseToTown",
		"Q256": "CloseToRegion",
		"Q257": "CloseToCountry",
		"Q258": "CloseToContinent",
		"Q259": "CloseToWorld",
		# "Q269": "Citizenship",
		# "Q270": "NFamilyMembers",
		# "Q274": "NChildren"
	},
}


def _select_source_file() -> Path:
	"""Prefer the filtered source, but fall back to the raw extract if needed."""

	if DEFAULT_SOURCE.exists():
		return DEFAULT_SOURCE
	if FALLBACK_SOURCE.exists():
		return FALLBACK_SOURCE
	raise FileNotFoundError(
		"Neither filtered nor raw WVS CSV found. "
		f"Looked for {DEFAULT_SOURCE.name} and {FALLBACK_SOURCE.name}."
	)


def _column_pairs() -> Iterator[Tuple[str, str]]:
	"""Yield (source, renamed) pairs for all predictor groups in order."""

	for mapping in COLUMN_GROUPS.values():
		for source_column, renamed in mapping.items():
			yield source_column, renamed


def _ordered_columns() -> list[str]:
	"""Return the expected output column order starting with the LifeSat target."""

	renamed_predictors = [renamed for _, renamed in _column_pairs()]
	return [TARGET_RENAMED, *renamed_predictors]


def _validate_columns(expected: Iterable[str], available: Iterable[str]) -> None:
	"""Ensure every requested WVS column is present in the source extract."""

	missing = set(expected) - set(available)
	if missing:
		formatted = ", ".join(sorted(missing))
		raise KeyError(
			"Missing columns in source CSV. Update the predictor lists or "
			f"regenerate the input. Missing: {formatted}"
		)


def build_dataset() -> pd.DataFrame:
	"""Load the WVS subset, rename Q49 to LifeSat, and enforce column order."""

	source = _select_source_file()
	predictor_pairs = list(_column_pairs())
	keep_columns = [TARGET_COLUMN, *(source_col for source_col, _ in predictor_pairs)]

	if len(keep_columns) == 1:
		raise ValueError("Populate COLUMN_GROUPS before running the script.")

	# Load only the required WVS columns to keep memory usage manageable.
	frame = pd.read_csv(source, usecols=lambda col: col in keep_columns)
	_validate_columns(keep_columns, frame.columns)

	rename_map = {TARGET_COLUMN: TARGET_RENAMED}
	rename_map.update({source_col: renamed for source_col, renamed in predictor_pairs})
	frame = frame.rename(columns=rename_map)
	frame = frame[_ordered_columns()]
	return frame


def main() -> int:
	try:
		dataset = build_dataset()
	except Exception as exc:  # surface configuration issues clearly
		print(f"Error while constructing SDT feature set: {exc}", file=sys.stderr)
		return 1

	dataset.to_csv(OUTPUT_FILE, index=False)
	print(f"Saved SDT subset with {dataset.shape[0]} rows to {OUTPUT_FILE.name}.")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
