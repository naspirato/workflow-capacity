"""Metrics aggregation for PR-check runs."""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timezone
from typing import Any, Callable

from workflow_capacity.pr_check import PrCheckRun, estimate_shard_count, parse_pr_shard_key, parse_ts
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


def normalize_percentiles(percentiles: float | int | list[float] | None) -> list[float]:
    if percentiles is None:
        return [90.0]
    if isinstance(percentiles, (int, float)):
        return [float(percentiles)]
    return sorted({float(p) for p in percentiles})


def pct_label(pct: float) -> str:
    if pct == int(pct):
        return f"p{int(pct)}"
    return "p" + str(pct).replace(".", "_")


def agg_get(cell: dict[str, Any], metric: str, pct: float) -> float | None:
    """Read wait/work/total from an aggregate cell at a given percentile."""
    return cell.get(f"{metric}_{pct_label(pct)}")


def aggregate_percentiles(
    rows: list[RunMetrics],
    percentiles: float | int | list[float] | None = None,
) -> dict[tuple[int | None, str], dict[str, Any]]:
    pcts = normalize_percentiles(percentiles)
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
        cell: dict[str, Any] = {
            "n": len(items),
            "wait_median": statistics.median(waits) if waits else None,
            "work_median": statistics.median(works) if works else None,
            "total_median": statistics.median(totals) if totals else None,
        }
        dates = {r.date_utc for r in items if r.date_utc is not None}
        if dates:
            cell["n_days"] = len(dates)
        for pct in pcts:
            pl = pct_label(pct)
            cell[f"wait_{pl}"] = percentile(waits, pct)
            cell[f"work_{pl}"] = percentile(works, pct)
            cell[f"total_{pl}"] = percentile(totals, pct)
        result[key] = cell
    return result


def aggregate_p90(rows: list[RunMetrics]) -> dict[tuple[int | None, str], dict[str, Any]]:
    return aggregate_percentiles(rows, [90.0])


@dataclass
class RunMetrics:
    run_id: int
    hour_utc: int
    d_key: str
    wait_min: float
    work_min: float
    date_utc: date | None = None

    @property
    def total_min(self) -> float:
        return self.wait_min + self.work_min


def run_id_from_event(e: AllocEvent) -> int | None:
    parsed = parse_pr_shard_key(e.key)
    if parsed is not None and parsed[0] == "rwdi":
        return parsed[1]
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
        started_utc = started.astimezone(timezone.utc)
        _, est_d = estimate_shard_count(
            float(run.rwdi_job["duration_sec"]),
            started_at=started,
            capacity_cap=12,
        )
        out.append(
            RunMetrics(
                run_id=rid,
                hour_utc=started_utc.hour,
                d_key=d_group(est_d),
                wait_min=vals["wait"] / 60.0,
                work_min=vals["work"] / 60.0,
                date_utc=started_utc.date(),
            )
        )
    return out


def _branch_metrics_sec(
    events: list[AllocEvent],
    pr_runs: list[PrCheckRun],
) -> tuple[dict[int, dict[str, float]], dict[int, dict[str, float]]]:
    """Per PR run: relwithdebinfo and release-asan wait/work (sec)."""
    rwdi_job_to_run = {str(r.rwdi_job["job_id"]): r.run_id for r in pr_runs}
    asan_job_to_run = {
        str(r.asan_job["job_id"]): r.run_id for r in pr_runs if r.asan_job
    }
    rwdi: dict[int, dict[str, float]] = defaultdict(lambda: {"wait": 0.0, "work": 0.0})
    asan: dict[int, dict[str, float]] = defaultdict(lambda: {"wait": 0.0, "work": 0.0})

    for e in events:
        parsed = parse_pr_shard_key(e.key)
        if parsed is not None:
            branch, rid = parsed
            bucket = rwdi if branch == "rwdi" else asan
            bucket[rid]["wait"] += e.wait_sec
            bucket[rid]["work"] += e.work_sec
            continue
        if not e.key.isdigit():
            continue
        if e.key in rwdi_job_to_run:
            rid = rwdi_job_to_run[e.key]
            rwdi[rid]["wait"] += e.wait_sec
            rwdi[rid]["work"] += e.work_sec
        elif e.key in asan_job_to_run:
            rid = asan_job_to_run[e.key]
            asan[rid]["wait"] += e.wait_sec
            asan[rid]["work"] += e.work_sec

    return rwdi, asan


def pr_wall_metrics_from_events(
    events: list[AllocEvent],
    pr_runs: list[PrCheckRun],
) -> list[RunMetrics]:
    """PR-check wall = max(relwithdebinfo, release-asan) — jobs start in parallel."""
    rwdi, asan = _branch_metrics_sec(events, pr_runs)
    out: list[RunMetrics] = []
    for run in pr_runs:
        started = parse_ts(run.rwdi_job["started_at"])
        if started.weekday() >= 5:
            continue
        started_utc = started.astimezone(timezone.utc)
        _, est_d = estimate_shard_count(
            float(run.rwdi_job["duration_sec"]),
            started_at=started,
            capacity_cap=12,
        )
        r = rwdi.get(run.run_id, {"wait": 0.0, "work": 0.0})
        a = asan.get(run.run_id, {"wait": 0.0, "work": 0.0})
        rw_total = r["wait"] + r["work"]
        aw_total = a["wait"] + a["work"]
        if aw_total >= rw_total and run.asan_job:
            wait_min = a["wait"] / 60.0
            work_min = a["work"] / 60.0
        else:
            wait_min = r["wait"] / 60.0
            work_min = r["work"] / 60.0
        if max(rw_total, aw_total) <= 0:
            continue
        out.append(
            RunMetrics(
                run_id=run.run_id,
                hour_utc=started_utc.hour,
                d_key=d_group(est_d),
                wait_min=wait_min,
                work_min=work_min,
                date_utc=started_utc.date(),
            )
        )
    return out


def scenario_metrics(
    baseline: ScenarioResult,
    parallel: ScenarioResult,
    pr_runs: list[PrCheckRun],
    *,
    shard_eligible: Callable[[PrCheckRun], bool] | None = None,
    pr_wall: bool = True,
) -> tuple[list[RunMetrics], list[RunMetrics]]:
    job_id_to_run = {str(r.rwdi_job["job_id"]): r.run_id for r in pr_runs}
    if shard_eligible is not None:
        eligible = {r.run_id for r in pr_runs if shard_eligible(r)}
        pr_subset = [r for r in pr_runs if r.run_id in eligible or r.mode != "sharded"]
    else:
        pr_subset = pr_runs
    if pr_wall:
        base_rows = pr_wall_metrics_from_events(baseline.alloc_events, pr_subset)
        par_rows = pr_wall_metrics_from_events(parallel.alloc_events, pr_subset)
    else:
        base_rows = metrics_from_events(baseline.alloc_events, pr_subset, job_id_to_run)
        par_rows = metrics_from_events(parallel.alloc_events, pr_subset, job_id_to_run)
    return base_rows, par_rows


def comparison_table(
    base_agg: dict[tuple[int | None, str], dict[str, Any]],
    par_agg: dict[tuple[int | None, str], dict[str, Any]],
    *,
    hours: list[int] | None = None,
    d_keys: list[str] | None = None,
    percentiles: float | int | list[float] | None = None,
    primary_percentile: float | None = None,
) -> list[dict[str, Any]]:
    pcts = normalize_percentiles(percentiles)
    primary = float(primary_percentile if primary_percentile is not None else pcts[-1])
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
            row: dict[str, Any] = {
                "hour_utc": f"{hour:02d}:00",
                "d_group": D_GROUP_LABELS.get(d_key, d_key),
                "n": max(b.get("n", 0), p.get("n", 0)),
                "n_days": max(b.get("n_days", 0), p.get("n_days", 0)),
            }
            for pct in pcts:
                pl = pct_label(pct)
                bw = agg_get(b, "wait", pct)
                bb = agg_get(b, "work", pct)
                bt = agg_get(b, "total", pct)
                pw = agg_get(p, "wait", pct)
                pb = agg_get(p, "work", pct)
                pt = agg_get(p, "total", pct)
                row[f"mono_wait_{pl}"] = bw
                row[f"mono_work_{pl}"] = bb
                row[f"mono_total_{pl}"] = bt
                row[f"shard_wait_{pl}"] = pw
                row[f"shard_work_{pl}"] = pb
                row[f"shard_total_{pl}"] = pt
                row[f"delta_wait_{pl}"] = (pw or 0) - (bw or 0)
                row[f"delta_work_{pl}"] = (pb or 0) - (bb or 0)
                row[f"delta_total_{pl}"] = (pt or 0) - (bt or 0)
                row[f"delta_total_pct_{pl}"] = (
                    100.0 * ((pt or 0) - (bt or 0)) / bt if bt else None
                )
            pl = pct_label(primary)
            row["delta_wait"] = row.get(f"delta_wait_{pl}")
            row["delta_work"] = row.get(f"delta_work_{pl}")
            row["delta_total"] = row.get(f"delta_total_{pl}")
            row["delta_total_pct"] = row.get(f"delta_total_pct_{pl}")
            rows.append(row)
    return rows
