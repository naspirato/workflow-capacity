#!/usr/bin/env python3
"""Collect job history (optional), run simulations, export JSON for the HTML page."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from workflow_capacity.cache import ensure_dataset, load_dataset
from workflow_capacity.compare import evaluate_config, results_to_dataframe
from workflow_capacity.config import PoolConfig
from workflow_capacity.metrics import D_GROUP_LABELS, ROLL_OUTS

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config" / "capacity.example.yml"
DEFAULT_OUTPUT = ROOT / "data" / "simulation_results.json"
PEAK_HOURS = list(range(9, 16))


def build_configs(base: PoolConfig) -> list[PoolConfig]:
    return [
        base,
        base.with_quota_scale(vcpu=1.1, name="vcpu+10%"),
        base.with_quota_scale(
            vcpu=1.25, instances=1.25, ram_gb=1.25, nrd_ssd_gb=1.25, name="all+25%"
        ),
        base.with_quota_scale(
            vcpu=2.0, instances=2.0, ram_gb=2.0, nrd_ssd_gb=2.0, name="all x2"
        ),
    ]


def _round_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, float):
            out[key] = round(value, 2)
        else:
            out[key] = value
    return out


def _metrics_pair(base_agg: dict, par_agg: dict, key: tuple) -> dict[str, Any] | None:
    b = base_agg.get(key)
    p = par_agg.get(key)
    if not b or not p:
        return None
    bw, bb, bt = b.get("wait_p90"), b.get("work_p90"), b.get("total_p90")
    pw, pb, pt = p.get("wait_p90"), p.get("work_p90"), p.get("total_p90")
    if bt is None or pt is None:
        return None
    return {
        "n": max(b.get("n", 0), p.get("n", 0)),
        "mono_wait_p90": bw,
        "mono_work_p90": bb,
        "mono_total_p90": bt,
        "shard_wait_p90": pw,
        "shard_work_p90": pb,
        "shard_total_p90": pt,
        "delta_wait": (pw or 0) - (bw or 0),
        "delta_work": (pb or 0) - (bb or 0),
        "delta_total": pt - bt,
        "delta_total_pct": 100.0 * (pt - bt) / bt if bt else None,
    }


def export_results(
    jobs: list[dict[str, Any]],
    *,
    config_path: Path,
    classify: bool,
    rollout_label: str,
    peak_hours: list[int],
) -> dict[str, Any]:
    from workflow_capacity.pr_check import build_pr_check_runs

    base = PoolConfig.load(config_path, name="current")
    configs = build_configs(base)
    rollout = next(r for r in ROLL_OUTS if r[1] == rollout_label)
    _, _, shard_eligible = rollout

    pr_runs = build_pr_check_runs(jobs, classify=classify)
    scenarios: list[dict[str, Any]] = []
    eval_results = []

    for cfg in configs:
        item = evaluate_config(
            jobs,
            pr_runs,
            cfg,
            rollout_label=rollout_label,
            shard_eligible=shard_eligible,
            peak_hours=peak_hours,
        )
        eval_results.append(item)
        overall = _metrics_pair(item.base_agg, item.par_agg, (None, "all"))
        by_d_group = []
        for d_key, d_label in D_GROUP_LABELS.items():
            if d_key == "all":
                continue
            row = _metrics_pair(item.base_agg, item.par_agg, (None, d_key))
            if row:
                row["d_group"] = d_label
                by_d_group.append(_round_row(row))

        scenarios.append(
            {
                "config": cfg.name,
                "vm_budget": round(cfg.max_instances_budget(), 1),
                "quotas": {k: int(v) for k, v in cfg.quotas.items()},
                "overall": _round_row(overall) if overall else None,
                "by_d_group": by_d_group,
                "by_hour": [_round_row(r) for r in item.table if r["d_group"] == "все D"],
            }
        )

    df = results_to_dataframe(eval_results)

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "jobs_count": len(jobs),
            "pr_runs_count": len(pr_runs),
            "classify": classify,
            "rollout": rollout_label,
            "peak_hours": peak_hours,
            "config_path": str(config_path),
        },
        "configs": [
            {
                "name": c.name,
                "instances": int(c.quotas["instances"]),
                "vcpu": int(c.quotas["vcpu"]),
                "ram_gb": int(c.quotas["ram_gb"]),
                "nrd_ssd_gb": int(c.quotas["nrd_ssd_gb"]),
                "vm_budget": round(c.max_instances_budget(), 1),
            }
            for c in configs
        ],
        "scenarios": scenarios,
        "rows": json.loads(df.round(2).to_json(orient="records")),
    }


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
    args = parser.parse_args()

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
        dataset = ensure_dataset(days=args.days, repo=args.repo, refresh=args.refresh)

    payload = export_results(
        dataset.jobs,
        config_path=args.config,
        classify=args.classify,
        rollout_label=args.rollout,
        peak_hours=PEAK_HOURS,
    )
    payload["meta"]["repo"] = dataset.repo
    payload["meta"]["since"] = dataset.since
    payload["meta"]["until"] = dataset.until
    payload["meta"]["dataset"] = dataset.path.name

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"wrote {args.output} ({len(payload['scenarios'])} scenarios, {len(dataset.jobs)} jobs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
