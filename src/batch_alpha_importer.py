#!/usr/bin/env python3
"""
Batch Alpha Importer

One-click tool: input a JSON file of alpha expressions, a region, and a category tag.
The script will:
  1. Load alpha expressions from the JSON file
  2. For each pre-configured universe in the target region, generate a simulatable
     alpha JSON with proper settings and save it as an intermediate file for review
  3. Automatically import all generated JSONs into the database

This replaces the previous manual workflow of:
  - Running utils.py for each universe (with region/universe/dataset parameters)
  - Manually importing each generated JSON with import_alpha_csv.py

Usage:
  # Basic usage: default settings (decay=10, delay=1, neutralization=REVERSION_AND_MOMENTUM)
  python src/batch_alpha_importer.py /path/to/alphas.json -r USA -t news
  python src/batch_alpha_importer.py /Users/tianyuan/.claude/skills/brain-expression-pipeline/out_put/news17_GLB_final_expressions.json -r GLB -t news

  # Specify priority (single value or range like "0-5")
  python src/batch_alpha_importer.py /path/to/alphas.json -r EUR -t analyst -p 1

  # Override universes (process only specific universes instead of all in the region)
  python src/batch_alpha_importer.py /path/to/alphas.json -r USA -t news -u TOP3000 TOP500

  # Generate JSONs only, skip DB import (review intermediate files first)
  python src/batch_alpha_importer.py /path/to/alphas.json -r GLB -t fundamental --no-import

  # Custom delay (number of days to lag data, commonly 0 or 1)
  python src/batch_alpha_importer.py /path/to/alphas.json -r USA -t news --delay 0

  # Custom decay (controls signal weight over time, higher = slower decay)
  python src/batch_alpha_importer.py /path/to/alphas.json -r USA -t news --decay 5

  # Custom neutralization method (e.g. MARKET, SUBINDUSTRY, NONE, etc.)
  python src/batch_alpha_importer.py /path/to/alphas.json -r USA -t news --neutralization MARKET

  # Custom truncation (winsorize extreme values, e.g. 0.05 = 5%)
  python src/batch_alpha_importer.py /path/to/alphas.json -r USA -t news --truncation 0.05

  # Custom max trade (ON/OFF, controls whether max trade weighting is applied)
  python src/batch_alpha_importer.py /path/to/alphas.json -r USA -t news --max-trade ON

  # Combine multiple custom settings
  python src/batch_alpha_importer.py /path/to/alphas.json -r USA -t news --delay 0 --decay 5 --neutralization MARKET --truncation 0.05 --max-trade ON
"""

import os
import sys
import json
import argparse
from datetime import datetime

# ============================================================
# Region -> Universe mapping
# Modify the lists below to change which universes are processed
# for each region.
# ============================================================
REGION_UNIVERSE_MAP = {
    "USA": ["TOP3000", "TOP2000", "TOP1000", "TOP500", "TOPSP500", "ILLIQUID_MINVOL1M"],
    "GLB": ["TOP3000", "MINVOL1M", "TOPDIV3000"],
    "EUR": ["TOP2500", "TOPCS1600", "TOP1200", "TOP800", "TOP400", "ILLIQUID_MINVOL1M"],
    "ASI": ["MINVOL1M", "ILLIQUID_MINVOL1M", "TOP500"],
    "HKG": ["TOP800", "TOP500"],
    "IND": ["TOP500"],
    "KOR": ["TOP600"],
    "CHN": ["TOP2000U"],
}

# Default simulation settings
DEFAULT_SETTINGS = {
    "delay": 1,
    "decay": 10,
    "neutralization": "REVERSION_AND_MOMENTUM",
    "truncation": 0.01,
    "maxTrade": "OFF",
}

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
_OUTPUT_DIR = os.path.join(_PROJECT_ROOT, "output", "pending_alpha")


def load_expressions(json_path):
    """
    Load alpha expressions from a JSON file.

    The file must contain a list. Each item can be:
      - A string (raw expression):  ["expr1", "expr2", ...]
      - A dict with a 'regular' key: [{"regular": "expr1", ...}, ...]

    Returns:
        list: The raw list from the JSON file.
    """
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    with open(json_path, 'r') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(
            f"JSON file must contain a list, got {type(data).__name__}. "
            f"Expected format: [\"expr1\", \"expr2\", ...] or "
            f"[{{\"regular\": \"expr1\", ...}}, ...]"
        )

    return data


def generate_alpha_entry(regular, region, universe, settings):
    """
    Build a single alpha entry dict for simulation.

    The entry contains:
      - type:         "REGULAR" (single alpha) or "SUPER" (combo alpha)
      - settings:     Simulation parameters (region, universe, delay, decay, etc.)
      - regular:      The alpha expression string in FASTEXPR language

    Key simulation settings explained:
      - delay:            Data lag (0 = same-day, 1 = previous day)
      - decay:            Weight decay factor for time-series signal
      - neutralization:   Risk neutralization method (MARKET, SUBINDUSTRY, NONE, etc.)
      - truncation:       Winsorization threshold to cap extreme values
      - pasteurization:   ON = apply pasteurization to remove outliers
      - unitHandling:     How to handle unit/scale mismatches (VERIFY, RESCALE, etc.)
      - nanHandling:      How to handle NaN values (OFF, FILL_ZERO, etc.)
      - maxTrade:         ON/OFF - apply max-trade weighting
      - testPeriod:       In-sample test period (P0Y = use all available history)
      - visualization:    Whether to generate PnL charts
    """
    return {
        "type": "REGULAR",
        "settings": {
            "instrumentType": "EQUITY",
            "region": region,
            "universe": universe,
            "delay": settings["delay"],
            "decay": settings["decay"],
            "neutralization": settings["neutralization"],
            "truncation": settings["truncation"],
            "pasteurization": "ON",
            "unitHandling": "VERIFY",
            "nanHandling": "OFF",
            "language": "FASTEXPR",
            "visualization": False,
            "testPeriod": "P0Y",
            "maxTrade": settings["maxTrade"],
        },
        "regular": regular,
    }


def generate_universe_json(expressions, region, universe, category_tag, settings):
    """
    Generate a simulatable_alphas JSON file for a single universe.

    Args:
        expressions: List loaded from the input JSON (strings or dicts).
        region: Region code (e.g. "USA").
        universe: Universe code (e.g. "TOP3000").
        category_tag: Label used in the filename (e.g. "news").
        settings: Dict of simulation settings.

    Returns:
        Absolute path to the generated JSON file.
    """
    alphas = []
    for item in expressions:
        # Support two input formats per item:
        #   1. Plain string:     "expression_string"
        #   2. Dict with keys:   {"regular": "expression_string", ...}
        if isinstance(item, str):
            regular = item
        elif isinstance(item, dict):
            # Try "regular" key first, fall back to "expression"
            regular = item.get("regular", item.get("expression", ""))
        else:
            continue  # Skip unrecognized types

        # Skip empty or invalid expressions
        if not regular or not isinstance(regular, str) or not regular.strip():
            continue

        alphas.append(generate_alpha_entry(regular, region, universe, settings))

    os.makedirs(_OUTPUT_DIR, exist_ok=True)

    filename = f"simulatable_alphas_{region}_{universe}_{settings['delay']}_{category_tag}.json"
    output_path = os.path.join(_OUTPUT_DIR, filename)

    with open(output_path, 'w') as f:
        json.dump(alphas, f, indent=4, ensure_ascii=False)

    return output_path


def import_json_to_db(json_path, priority_str):
    """
    Import a generated JSON file into the database.

    Reuses the core logic from import_alpha_csv.py (read_json_file,
    assign_priorities, AlphaListPendingSimulatedDAO).

    Args:
        json_path: Path to the generated simulatable_alphas JSON.
        priority_str: Priority string (e.g. "0" or "0-5").

    Returns:
        Number of records inserted.
    """
    # Ensure src/ is on the import path
    if _SCRIPT_DIR not in sys.path:
        sys.path.insert(0, _SCRIPT_DIR)

    from import_alpha_csv import read_json_file, assign_priorities
    from dao import AlphaListPendingSimulatedDAO
    from logger import Logger

    logger = Logger().logger

    # Parse priority string into a list of levels:
    #   "0"     -> [0]           (single priority)
    #   "0-5"   -> [0,1,2,3,4,5] (range, inclusive)
    if '-' in priority_str:
        try:
            min_p, max_p = map(int, priority_str.split('-'))
            if min_p < 0 or max_p < min_p:
                raise ValueError
            priority_levels = list(range(min_p, max_p + 1))
        except (ValueError, TypeError):
            logger.error(f"Invalid priority range: {priority_str}")
            return 0
    else:
        try:
            p = int(priority_str)
            if p < 0:
                raise ValueError
            priority_levels = [p]
        except ValueError:
            logger.error(f"Invalid priority value: {priority_str}")
            return 0

    alphas = read_json_file(json_path)
    if not alphas:
        logger.warning(f"No alphas found in {json_path}, skipping import")
        return 0

    # Assign a priority level to each alpha from the configured list
    priorities = assign_priorities(alphas, priority_levels)

    # Build DB records with metadata (type, settings, region, timestamp)
    db_records = []
    for alpha, priority in zip(alphas, priorities):
        region = alpha.get('region', '')
        db_records.append({
            'type': alpha.get('type', 'REGULAR'),
            'settings': alpha.get('settings', '{}'),
            'regular': alpha.get('regular', ''),
            'priority': priority,
            'region': region,
            'created_at': datetime.now(),
        })

    dao = AlphaListPendingSimulatedDAO()
    inserted = dao.batch_insert(db_records)
    return inserted


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Batch Alpha Importer - generate & import alphas for all universes "
            "in a region with a single command"
        )
    )
    parser.add_argument(
        "json_path",
        help="Path to the JSON file containing alpha expressions (must be a list)"
    )
    parser.add_argument(
        "-r", "--region", required=True,
        help="Target region: %s" % ", ".join(sorted(REGION_UNIVERSE_MAP.keys()))
    )
    parser.add_argument(
        "-t", "--tag", required=True,
        help="Category tag used in filenames (e.g. news, analyst, fundamental)"
    )
    parser.add_argument(
        "-p", "--priority", default="0",
        help="Priority for DB import: single value '0' or range '0-5'. Default: 0"
    )
    parser.add_argument(
        "-u", "--universes", nargs="+",
        help=(
            "Override universes to process. If omitted, all universes from the "
            "region mapping are used. Example: -u TOP3000 TOP500"
        )
    )
    parser.add_argument(
        "--decay", type=int, default=DEFAULT_SETTINGS["decay"],
        help=f"Decay value (default: {DEFAULT_SETTINGS['decay']})"
    )
    parser.add_argument(
        "--delay", type=int, default=DEFAULT_SETTINGS["delay"],
        help=f"Data delay (default: {DEFAULT_SETTINGS['delay']})"
    )
    parser.add_argument(
        "--neutralization", default=DEFAULT_SETTINGS["neutralization"],
        help=f"Neutralization method (default: {DEFAULT_SETTINGS['neutralization']})"
    )
    parser.add_argument(
        "--truncation", type=float, default=DEFAULT_SETTINGS["truncation"],
        help=f"Truncation value (default: {DEFAULT_SETTINGS['truncation']})"
    )
    parser.add_argument(
        "--max-trade", default=DEFAULT_SETTINGS["maxTrade"],
        help=f"Max trade setting (default: {DEFAULT_SETTINGS['maxTrade']})"
    )
    parser.add_argument(
        "--no-import", action="store_true",
        help="Only generate intermediate JSONs, skip database import"
    )

    args = parser.parse_args()

    # ================================================================
    # Validate and resolve inputs
    # ================================================================

    # Normalize region to uppercase (user may input "usa", "Usa", etc.)
    region = args.region.upper()
    if region not in REGION_UNIVERSE_MAP:
        print(f"ERROR: Unknown region '{region}'.")
        print(f"Known regions: {', '.join(sorted(REGION_UNIVERSE_MAP.keys()))}")
        sys.exit(1)

    # Resolve universes: use user-specified list if provided,
    # otherwise use all universes from the region mapping
    if args.universes:
        universes = args.universes
        known = set(REGION_UNIVERSE_MAP[region])
        unknown = [u for u in universes if u not in known]
        if unknown:
            print(f"WARNING: {unknown} not in known mapping for {region} ({sorted(known)})")
    else:
        universes = REGION_UNIVERSE_MAP[region]

    # Build simulation settings dict (used by every generated alpha entry)
    settings = {
        "delay": args.delay,
        "decay": args.decay,
        "neutralization": args.neutralization,
        "truncation": args.truncation,
        "maxTrade": args.max_trade,
    }

    # ================================================================
    # Print summary
    # ================================================================
    print()
    print("=" * 60)
    print("  Batch Alpha Importer")
    print("=" * 60)
    print(f"  Input:       {args.json_path}")
    print(f"  Region:      {region}")
    print(f"  Tag:         {args.tag}")
    print(f"  Universes:   {', '.join(universes)}")
    print(f"  Priority:    {args.priority}")
    print(f"  Settings:    delay={settings['delay']}, decay={settings['decay']}, "
          f"neut={settings['neutralization']}, trunc={settings['truncation']}, "
          f"maxTrade={settings['maxTrade']}")
    print(f"  DB Import:   {'SKIP' if args.no_import else 'YES'}")
    print("=" * 60)

    # ================================================================
    # Step 1: Load input
    # ================================================================
    print("\n[Step 1] Loading expressions...")
    try:
        expressions = load_expressions(args.json_path)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"  Loaded {len(expressions)} expression items")

    # ================================================================
    # Step 2: Generate intermediate JSONs for each universe
    # ================================================================
    print(f"\n[Step 2] Generating JSONs for {len(universes)} universe(s)...")
    generated_files = []
    for universe in universes:
        path = generate_universe_json(
            expressions, region, universe, args.tag, settings
        )
        generated_files.append(path)

        # Count expressions actually written
        with open(path, 'r') as f:
            count = len(json.load(f))
        print(f"  [{universe}] -> {os.path.basename(path)} ({count} alphas)")

    print(f"  Done. {len(generated_files)} intermediate file(s) saved.")

    # ================================================================
    # Step 3: Import to database
    # ================================================================
    if args.no_import:
        print("\n[Step 3] Skipping DB import (--no-import)")
        print("  To import manually:")
        for f in generated_files:
            print(f"    python src/import_alpha_csv.py {f} --priority {args.priority}")
    else:
        print(f"\n[Step 3] Importing to database...")
        total = 0
        for file_path in generated_files:
            # Import each generated JSON into the DB (one per universe)
            print(f"  Importing: {os.path.basename(file_path)} ... ", end="", flush=True)
            inserted = import_json_to_db(file_path, args.priority)
            total += inserted
            print(f"{inserted} records")
        print(f"  Total: {total} records inserted into database")

    print(f"\n{'=' * 60}")
    print("  All done!")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
