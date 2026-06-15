"""Discrete-event replay of the shared runner pool."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import timedelta, timezone
from typing import Any, Callable

from workflow_capacity.config import PoolConfig
from workflow_capacity.pool import PoolSimulator
from workflow_capacity.pr_check import (
    PrCheckRun,
    build_pr_check_runs,
    parse_pr_shard_key,
    parse_ts,
    sharded_rwdi_timeline,
)


@dataclass
class WorkItem:
    start: float
    preset: str
    key: str
    duration_sec: float
    parallel_count: int = 1
    is_pr_rwdi: bool = False
    is_pr_check: bool = False


@dataclass
class AllocEvent:
    requested_at: float
    started_at: float
    ended_at: float
    wait_sec: float
    work_sec: float
    preset: str
    parallel_count: int
    is_pr_rwdi: bool
    is_pr_check: bool
    key: str


@dataclass
class ScenarioResult:
    name: str
    config_name: str
    pool: PoolSimulator
    pr_wall_times: list[float]
    pr_queue_waits: list[float]
    pr_sharded: int
    pr_single: int
    pr_runner_seconds: float
    window_start: float
    window_end: float
    alloc_events: list[AllocEvent]

    @property
    def horizon_sec(self) -> float:
        return max(self.window_end - self.window_start, 1.0)


def job_start_epoch(job: dict[str, Any]) -> float:
    return parse_ts(job["started_at"]).timestamp()


PR_LOAD_STAGGER_SEC = 90.0


def drop_pr_run(run_id: int, load: float) -> bool:
    """Whether to drop a PR-check run when load < 1 (deterministic subsample)."""
    if load >= 1.0 - 1e-9:
        return False
    drop_frac = 1.0 - load
    step = max(1, round(1 / drop_frac))
    return run_id % step == 0


def extra_pr_replicas(run_id: int, load: float) -> int:
    """How many additional PR-check run copies to inject (deterministic)."""
    if load <= 1.0 + 1e-9:
        return 0
    extras = max(0, int(load) - 1)
    frac = load - int(load)
    if frac > 1e-9:
        step = max(1, round(1 / frac))
        if run_id % step == 0:
            extras += 1
    return extras


def _shift_started_at(iso: str, delta_sec: float) -> str:
    dt = parse_ts(iso).astimezone(timezone.utc) + timedelta(seconds=delta_sec)
    text = dt.isoformat()
    return text.replace("+00:00", "Z")


def scale_pr_traffic(
    jobs: list[dict[str, Any]],
    pr_runs: list[PrCheckRun],
    load: float,
) -> tuple[list[dict[str, Any]], list[PrCheckRun]]:
    """Scale PR-check arrival rate up (copies) or down (deterministic subsample)."""
    if abs(load - 1.0) < 1e-9:
        return jobs, pr_runs

    pr_run_ids = {r.run_id for r in pr_runs}
    if load < 1.0:
        kept_runs = [r for r in pr_runs if not drop_pr_run(r.run_id, load)]
        kept_ids = {r.run_id for r in kept_runs}
        out_jobs = [
            job
            for job in jobs
            if job.get("run_id") not in pr_run_ids or int(job["run_id"]) in kept_ids
        ]
        return out_jobs, kept_runs

    jobs_by_run: dict[int, list[dict[str, Any]]] = {}
    other_jobs: list[dict[str, Any]] = []
    for job in jobs:
        rid = job.get("run_id")
        if rid in pr_run_ids:
            jobs_by_run.setdefault(int(rid), []).append(job)
        else:
            other_jobs.append(job)

    max_run_id = max(r.run_id for r in pr_runs) if pr_runs else 0
    max_job_id = max(int(j["job_id"]) for j in jobs) if jobs else 0

    out_jobs = list(other_jobs)
    for run in pr_runs:
        out_jobs.extend(jobs_by_run.get(run.run_id, []))
    out_runs: list[PrCheckRun] = list(pr_runs)

    for run in pr_runs:
        extras = extra_pr_replicas(run.run_id, load)
        for replica in range(1, extras + 1):
            max_run_id += 1
            offset = 0.0 if replica == 1 else (replica - 1) * PR_LOAD_STAGGER_SEC
            run_jobs = jobs_by_run.get(run.run_id, [])
            cloned_jobs: list[dict[str, Any]] = []
            rwdi_job: dict[str, Any] | None = None
            asan_job: dict[str, Any] | None = None
            for job in run_jobs:
                max_job_id += 1
                clone = copy.copy(job)
                clone["job_id"] = max_job_id
                clone["run_id"] = max_run_id
                clone["started_at"] = _shift_started_at(job["started_at"], offset)
                cloned_jobs.append(clone)
                name = job["job_name"]
                if "relwithdebinfo" in name:
                    rwdi_job = clone
                elif "release-asan" in name or "asan" in name:
                    asan_job = clone
            if rwdi_job is None:
                continue
            out_jobs.extend(cloned_jobs)
            out_runs.append(
                PrCheckRun(
                    run_id=max_run_id,
                    pr_number=run.pr_number,
                    rwdi_job=rwdi_job,
                    asan_job=asan_job,
                    mode=run.mode,
                    shard_count=run.shard_count,
                    rwdi_wall_sec=run.rwdi_wall_sec,
                )
            )

    return out_jobs, out_runs


def is_pr_check_job(job: dict[str, Any]) -> bool:
    return job["workflow_name"] == "PR-check" and (
        "relwithdebinfo" in job["job_name"] or "asan" in job["job_name"]
    )


def is_pr_check_rwdi(job: dict[str, Any]) -> bool:
    return job["workflow_name"] == "PR-check" and "relwithdebinfo" in job["job_name"]


def is_pr_check_asan(job: dict[str, Any]) -> bool:
    return job["workflow_name"] == "PR-check" and (
        "release-asan" in job["job_name"] or "asan" in job["job_name"]
    )


def is_pr_check_sharded_job(job: dict[str, Any]) -> bool:
    return is_pr_check_rwdi(job) or is_pr_check_asan(job)


def _append_sharded_branch(
    items: list[WorkItem],
    *,
    run_id: int,
    branch: str,
    job: dict[str, Any],
    is_pr_rwdi: bool,
) -> None:
    start = job_start_epoch(job)
    mono = float(job["duration_sec"])
    _, shard_count, prepare_sec, shard_sec = sharded_rwdi_timeline(
        mono,
        started_at=parse_ts(job["started_at"]),
        capacity_cap=12,
    )
    if shard_count <= 1:
        items.append(
            WorkItem(
                start=start,
                preset=job["preset"],
                key=str(job["job_id"]),
                duration_sec=mono,
                is_pr_rwdi=is_pr_rwdi,
                is_pr_check=True,
            )
        )
        return
    items.append(
        WorkItem(
            start=start,
            preset=job["preset"],
            key=f"prepare:{branch}:{run_id}",
            duration_sec=prepare_sec,
            is_pr_rwdi=is_pr_rwdi,
            is_pr_check=True,
        )
    )
    items.append(
        WorkItem(
            start=start + prepare_sec,
            preset=job["preset"],
            key=f"shards:{branch}:{run_id}",
            duration_sec=shard_sec,
            parallel_count=shard_count,
            is_pr_rwdi=is_pr_rwdi,
            is_pr_check=True,
        )
    )


def build_work_items(
    jobs: list[dict[str, Any]],
    pr_runs: list[PrCheckRun],
    *,
    parallel: bool,
    shard_eligible: Callable[[PrCheckRun], bool] | None = None,
) -> list[WorkItem]:
    sharded_run_ids = {
        run.run_id
        for run in pr_runs
        if parallel
        and run.mode == "sharded"
        and (shard_eligible is None or shard_eligible(run))
    }
    items: list[WorkItem] = []

    for job in jobs:
        if parallel and job["run_id"] in sharded_run_ids and is_pr_check_sharded_job(job):
            continue
        start = job_start_epoch(job)
        duration = float(job["duration_sec"])
        items.append(
            WorkItem(
                start=start,
                preset=job["preset"],
                key=str(job["job_id"]),
                duration_sec=duration,
                is_pr_rwdi=is_pr_check_rwdi(job),
                is_pr_check=is_pr_check_job(job),
            )
        )

    if parallel:
        for run in pr_runs:
            if run.mode != "sharded":
                continue
            if shard_eligible is not None and not shard_eligible(run):
                continue
            wall, shard_count, _, _ = sharded_rwdi_timeline(
                float(run.rwdi_job["duration_sec"]),
                started_at=parse_ts(run.rwdi_job["started_at"]),
                capacity_cap=12,
            )
            run.shard_count = shard_count
            run.rwdi_wall_sec = wall
            _append_sharded_branch(
                items,
                run_id=run.run_id,
                branch="rwdi",
                job=run.rwdi_job,
                is_pr_rwdi=True,
            )
            if run.asan_job:
                _append_sharded_branch(
                    items,
                    run_id=run.run_id,
                    branch="asan",
                    job=run.asan_job,
                    is_pr_rwdi=False,
                )

    return sorted(items, key=lambda item: item.start)


def replay(
    items: list[WorkItem],
    config: PoolConfig,
    *,
    name: str,
    pr_runs: list[PrCheckRun],
    parallel: bool,
) -> ScenarioResult:
    pool = PoolSimulator(config=config)
    pr_walls: list[float] = []
    pr_waits: list[float] = []
    pr_runner_seconds = 0.0
    sharded = single = 0
    alloc_events: list[AllocEvent] = []
    window_start = items[0].start if items else 0.0
    window_end = window_start
    pr_partial: dict[str, dict[str, float]] = {}

    for item in items:
        parsed = parse_pr_shard_key(item.key)
        if parallel and parsed is not None and parsed[0] == "shards":
            branch, run_id = parsed
            run = next(r for r in pr_runs if r.run_id == run_id)
            job = run.rwdi_job if branch == "rwdi" else run.asan_job
            if job is None:
                continue
            cap = pool.capacity_cap(item.preset)
            mono = float(job["duration_sec"])
            _, shard_count, prepare_sec, shard_sec = sharded_rwdi_timeline(
                mono,
                started_at=parse_ts(job["started_at"]),
                capacity_cap=cap,
            )
            if branch == "rwdi":
                run.shard_count = shard_count
            item.parallel_count = shard_count
            item.duration_sec = shard_sec
            prep_key = f"prepare:{branch}:{run_id}"
            for other in items:
                if other.key == prep_key:
                    other.duration_sec = prepare_sec
                    break

        if item.parallel_count == 1:
            wait = pool.allocate(item.start, item.duration_sec, item.preset, item.key)
            end = item.start + wait + item.duration_sec
        else:
            wait = pool.allocate_parallel(
                item.start,
                item.duration_sec,
                item.preset,
                item.parallel_count,
                item.key,
            )
            end = item.start + wait + item.duration_sec

        alloc_events.append(
            AllocEvent(
                requested_at=item.start,
                started_at=item.start + wait,
                ended_at=end,
                wait_sec=wait,
                work_sec=item.duration_sec,
                preset=item.preset,
                parallel_count=item.parallel_count,
                is_pr_rwdi=item.is_pr_rwdi,
                is_pr_check=item.is_pr_check,
                key=item.key,
            )
        )

        window_end = max(window_end, end)
        if not item.is_pr_rwdi:
            continue

        parsed = parse_pr_shard_key(item.key)
        if parsed is not None:
            branch, run_id = parsed
            partial_key = f"{branch}:{run_id}"
            if item.key.startswith("prepare:"):
                pr_partial[partial_key] = {
                    "wait": wait,
                    "start": item.start,
                    "prepare": item.duration_sec,
                }
                pr_runner_seconds += item.duration_sec
            elif item.key.startswith("shards:"):
                partial = pr_partial.get(
                    partial_key,
                    {"wait": 0.0, "start": item.start, "prepare": 0.0},
                )
                wall = partial["prepare"] + item.duration_sec + partial["wait"] + wait
                pr_walls.append(wall)
                pr_waits.append(partial["wait"] + wait)
                pr_runner_seconds += item.parallel_count * item.duration_sec
                sharded += 1
                if branch == "rwdi":
                    for run in pr_runs:
                        if run.run_id == run_id:
                            run.rwdi_wall_sec = wall
                            break
            continue

        if item.is_pr_rwdi:
            pr_walls.append(item.duration_sec + wait)
            pr_waits.append(wait)
            pr_runner_seconds += item.duration_sec
            single += 1
            if item.key.isdigit():
                for run in pr_runs:
                    if str(run.rwdi_job["job_id"]) == item.key:
                        run.rwdi_wall_sec = item.duration_sec + wait
                        break

    pool.finalize(window_end)
    return ScenarioResult(
        name=name,
        config_name=config.name,
        pool=pool,
        pr_wall_times=pr_walls,
        pr_queue_waits=pr_waits,
        pr_sharded=sharded,
        pr_single=single,
        pr_runner_seconds=pr_runner_seconds,
        window_start=window_start,
        window_end=window_end,
        alloc_events=alloc_events,
    )


def run_pair(
    jobs: list[dict[str, Any]],
    pr_runs: list[PrCheckRun],
    config: PoolConfig,
    *,
    shard_eligible: Callable[[PrCheckRun], bool] | None = None,
    load_scale: float = 1.0,
) -> tuple[ScenarioResult, ScenarioResult]:
    sim_jobs, sim_runs = scale_pr_traffic(jobs, pr_runs, load_scale)
    baseline = replay(
        build_work_items(sim_jobs, sim_runs, parallel=False, shard_eligible=shard_eligible),
        config,
        name="monolith",
        pr_runs=sim_runs,
        parallel=False,
    )
    parallel = replay(
        build_work_items(sim_jobs, sim_runs, parallel=True, shard_eligible=shard_eligible),
        config,
        name="sharding",
        pr_runs=sim_runs,
        parallel=True,
    )
    return baseline, parallel
