#!/usr/bin/env python3
"""Run simulations and export JSON for the comparison HTML page."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from workflow_capacity.cache import ensure_dataset, load_dataset, resolve_dataset
from workflow_capacity.export_comparison import (
    DEFAULT_CONFIG,
    export_from_dataset,
    write_comparison_payload,
)

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "data" / "simulation_results.json"
PEAK_HOURS = list(range(8, 19))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, help="Path to jobs_*.json cache file")
    parser.add_argument("--collect", action="store_true", help="Collect from GitHub if missing")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--repo", default="ydb-platform/ydb")
    parser.add_argument("--workflows", default="PR-check", help="Comma-separated workflow names")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--classify", action="store_true")
    parser.add_argument("--rollout", default="all eligible")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--percentile",
        type=float,
        default=None,
        help="Single percentile for metrics (default: 90)",
    )
    parser.add_argument(
        "--percentiles",
        type=str,
        default=None,
        help="Comma-separated percentiles, e.g. 50,90,95 (overrides --percentile)",
    )
    parser.add_argument(
        "--primary-percentile",
        type=float,
        default=None,
        help="Primary percentile for deltas and HTML default (default: last in list)",
    )
    args = parser.parse_args()

    if args.percentiles:
        percentiles = [float(x.strip()) for x in args.percentiles.split(",") if x.strip()]
    elif args.percentile is not None:
        percentiles = args.percentile
    else:
        percentiles = None

    if args.data:
        dataset = load_dataset(args.data)
    elif args.collect:
        until = datetime.now(timezone.utc)
        since = until - timedelta(days=args.days)
        cache_dir = ROOT / "data" / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        slug = args.repo.replace("/", "_")
        path = cache_dir / f"jobs_{slug}_{since.date()}_{until.date()}.json"
        if path.exists() and not args.refresh:
            dataset = load_dataset(path)
        else:
            from workflow_capacity.collect import collect_window

            workflows = [w.strip() for w in args.workflows.split(",") if w.strip()]
            collect_window(
                repo=args.repo,
                since=since,
                until=until,
                output=path,
                workflows=workflows or None,
            )
            dataset = load_dataset(path)
    else:
        dataset = resolve_dataset(
            days=args.days,
            repo=args.repo,
            cache_dir=ROOT / "data" / "cache",
            refresh=args.refresh,
        )

    payload = export_from_dataset(
        dataset,
        config_path=args.config,
        classify=args.classify,
        rollout_label=args.rollout,
        peak_hours=PEAK_HOURS,
        percentiles=percentiles,
        primary_percentile=args.primary_percentile,
    )

    if args.output == DEFAULT_OUTPUT:
        paths = write_comparison_payload(payload, root=ROOT)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            __import__("json").dumps(payload, indent=2),
            encoding="utf-8",
        )
        paths = [args.output]

    print(
        f"wrote {paths[0]} "
        f"({len(payload['scenarios'])} scenarios, {len(payload['interactive'])} interactive points)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
