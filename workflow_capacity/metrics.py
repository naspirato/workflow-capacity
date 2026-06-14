"""Metrics aggregation for PR-check runs."""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import timezone
from typing import Any, Callable

from workflow_capacity.pr_check import PrCheckRun, estimate_shard_count, parse_ts
from workflow_capacity.simulate import AllocEvent, ScenarioResult

D_GROUPS = (
    ("d_lt_60", "D < 60"),
    ("d_60_120", "60 ≤ D < 120"),
    ("d_120_200", "120 ≤ D < 200"),
    ("d_gte_200", "D ≥ 200"),
    ("all", "все D"),
)

D_GROUP_LABELS = {key: label for key, label in D_GROUPS}

ROLL_OUTS = (
    ("all", "all eligible", None),
    ("main_stable", "main + stable/*", lambda r: (r.rwdi_job.get("base_ref") or "") == "main"
     or str(r.rwdi_job.get("base_ref") or "").startswith("stable-")),
    ("main_only", "main only", lambda r: (r.rwdi_job.get("base_ref") or "") == "main"),
)


def d_group(estimated_d_min: float) -> str:
    if estimated_d_min < 60:
        return "d_lt_60"
    if estimated_d_min < 120:
        return "d_60_120"
    if estimated_d_min < 200:
        return "d_120_200"
    return "d_gte_200"


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = int(round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[idx]


@dataclass
class RunMetrics:
    run_id: int
    hour_utc: int
    d_key: str
    wait_min: float
    work_min: float

    @property
    def total_min(self) -> float:
        return self.wait_min + self.work_min


def run_id_from_event(e: AllocEvent) -> int | None:
    if e.key.startswith("prepare:") or e.key.startswith("shards:"):
        return int(e.key.split(":")[1])
    return None


def metrics_from_events(
    events: list[AllocEvent],
    pr_runs: list[PrCheckRun],
    job_id_to_run: dict[str, int],
) -> list[RunMetrics]:
    by_run: dict[int, dict[str, float]] = defaultdict(lambda: {"wait": 0.0, "work": 0.0})
    for e in events:
        if not e.is_pr_rwdi:
            continue
        rid = run_id_from_event(e)
        if rid is None and e.key.isdigit():
            rid = job_id_to_run.get(e.key)
        if rid is None:
            continue
        by_run[rid]["wait"] += e.wait_sec
        by_run[rid]["work"] += e.work_sec

    run_by_id = {r.run_id: r for r in pr_runs}
    out: list[RunMetrics] = []
    for rid, vals in by_run.items():
        run = run_by_id.get(rid)
        if not run:
            continue
        started = parse_ts(run.rwdi_job["started_at"])
        if started.weekday() >= 5:
            continue
        _, est_d = estimate_shard_count(
            float(run.rwdi_job["duration_sec"]),
            started_at=started,
            capacity_cap=12,
        )
        out.append(
            RunMetrics(
                run_id=rid,
                hour_utc=started.astimezone(timezone.utc).hour,
                d_key=d_group(est_d),
                wait_min=vals["wait"] / 60.0,
                work_min=vals["work"] / 60.0,
            )
        )
    return out


def aggregate_p90(rows: list[RunMetrics]) -> dict[tuple[int | None, str], dict[str, Any]]:
    cells: dict[tuple[int | None, str], list[RunMetrics]] = defaultdict(list)
    for r in rows:
        cells[(r.hour_utc, r.d_key)].append(r)
        cells[(r.hour_utc, "all")].append(r)
        cells[(None, r.d_key)].append(r)
        cells[(None, "all")].append(r)

    result: dict[tuple[int | None, str], dict[str, Any]] = {}
    for key, items in cells.items():
        waits = [x.wait_min for x in items]
        works = [x.work_min for x in items]
        totals = [x.total_min for x in items]
        result[key] = {
            "n": len(items),
            "wait_p90": percentile(waits, 90),
            "work_p90": percentile(works, 90),
            "total_p90": percentile(totals, 90),
            "wait_median": statistics.median(waits) if waits else None,
            "work_median": statistics.median(works) if works else None,
            "total_median": statistics.median(totals) if totals else None,
        }
    return result


def scenario_metrics(
    baseline: ScenarioResult,
    parallel: ScenarioResult,
    pr_runs: list[PrCheckRun],
    *,
    shard_eligible: Callable[[PrCheckRun], bool] | None = None,
) -> tuple[list[RunMetrics], list[RunMetrics]]:
    job_id_to_run = {str(r.rwdi_job["job_id"]): r.run_id for r in pr_runs}
    if shard_eligible is not None:
        eligible = {r.run_id for r in pr_runs if shard_eligible(r)}
        pr_subset = [r for r in pr_runs if r.run_id in eligible or r.mode != "sharded"]
    else:
        pr_subset = pr_runs
    base_rows = metrics_from_events(baseline.alloc_events, pr_subset, job_id_to_run)
    par_rows = metrics_from_events(parallel.alloc_events, pr_subset, job_id_to_run)
    return base_rows, par_rows


def comparison_table(
    base_agg: dict[tuple[int | None, str], dict[str, Any]],
    par_agg: dict[tuple[int | None, str], dict[str, Any]],
    *,
    hours: list[int] | None = None,
    d_keys: list[str] | None = None,
) -> list[dict[str, Any]]:
    hours = hours if hours is not None else list(range(24))
    d_keys = d_keys if d_keys is not None else [k for k, _ in D_GROUPS]
    rows: list[dict[str, Any]] = []
    for hour in hours:
        for d_key in d_keys:
            key = (hour, d_key)
            b = base_agg.get(key) or base_agg.get((None, d_key))
            p = par_agg.get(key) or par_agg.get((None, d_key))
            if not b or not p:
                continue
            rows.append(
                {
                    "hour_utc": f"{hour:02d}:00",
                    "d_group": D_GROUP_LABELS.get(d_key, d_key),
                    "n": max(b.get("n", 0), p.get("n", 0)),
                    "mono_wait_p90": b["wait_p90"],
                    "mono_work_p90": b["work_p90"],
                    "mono_total_p90": b["total_p90"],
                    "shard_wait_p90": p["wait_p90"],
                    "shard_work_p90": p["work_p90"],
                    "shard_total_p90": p["total_p90"],
                    "delta_wait": (p["wait_p90"] or 0) - (b["wait_p90"] or 0),
                    "delta_work": (p["work_p90"] or 0) - (b["work_p90"] or 0),
                    "delta_total": (p["total_p90"] or 0) - (b["total_p90"] or 0),
                    "delta_total_pct": (
                        100.0 * ((p["total_p90"] or 0) - (b["total_p90"] or 0)) / b["total_p90"]
                        if b.get("total_p90")
                        else None
                    ),
                }
            )
    return rows
