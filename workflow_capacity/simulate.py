"""Discrete-event replay of the shared runner pool."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from workflow_capacity.config import PoolConfig
from workflow_capacity.pool import PoolSimulator
from workflow_capacity.pr_check import PrCheckRun, build_pr_check_runs, parse_ts, sharded_rwdi_timeline


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


def is_pr_check_job(job: dict[str, Any]) -> bool:
    return job["workflow_name"] == "PR-check" and (
        "relwithdebinfo" in job["job_name"] or "asan" in job["job_name"]
    )


def is_pr_check_rwdi(job: dict[str, Any]) -> bool:
    return job["workflow_name"] == "PR-check" and "relwithdebinfo" in job["job_name"]


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
        if parallel and job["run_id"] in sharded_run_ids and is_pr_check_rwdi(job):
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
            start = job_start_epoch(run.rwdi_job)
            mono = float(run.rwdi_job["duration_sec"])
            wall, shard_count, prepare_sec, shard_sec = sharded_rwdi_timeline(
                mono,
                started_at=parse_ts(run.rwdi_job["started_at"]),
                capacity_cap=12,
            )
            run.shard_count = shard_count
            run.rwdi_wall_sec = wall
            if shard_count <= 1:
                items.append(
                    WorkItem(
                        start=start,
                        preset=run.rwdi_job["preset"],
                        key=str(run.rwdi_job["job_id"]),
                        duration_sec=mono,
                        is_pr_rwdi=True,
                        is_pr_check=True,
                    )
                )
                continue
            items.append(
                WorkItem(
                    start=start,
                    preset=run.rwdi_job["preset"],
                    key=f"prepare:{run.run_id}",
                    duration_sec=prepare_sec,
                    is_pr_rwdi=True,
                    is_pr_check=True,
                )
            )
            items.append(
                WorkItem(
                    start=start + prepare_sec,
                    preset=run.rwdi_job["preset"],
                    key=f"shards:{run.run_id}",
                    duration_sec=shard_sec,
                    parallel_count=shard_count,
                    is_pr_rwdi=True,
                    is_pr_check=True,
                )
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
        if parallel and item.key.startswith("shards:"):
            run_id = int(item.key.split(":")[1])
            run = next(r for r in pr_runs if r.run_id == run_id)
            cap = pool.capacity_cap(item.preset)
            mono = float(run.rwdi_job["duration_sec"])
            _, shard_count, prepare_sec, shard_sec = sharded_rwdi_timeline(
                mono,
                started_at=parse_ts(run.rwdi_job["started_at"]),
                capacity_cap=cap,
            )
            run.shard_count = shard_count
            item.parallel_count = shard_count
            item.duration_sec = shard_sec
            prep_key = f"prepare:{run_id}"
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

        if item.key.startswith("prepare:"):
            run_id = item.key.split(":")[1]
            pr_partial[run_id] = {"wait": wait, "start": item.start, "prepare": item.duration_sec}
            pr_runner_seconds += item.duration_sec
        elif item.key.startswith("shards:"):
            run_id = item.key.split(":")[1]
            partial = pr_partial.get(run_id, {"wait": 0.0, "start": item.start, "prepare": 0.0})
            wall = partial["prepare"] + item.duration_sec + partial["wait"] + wait
            pr_walls.append(wall)
            pr_waits.append(partial["wait"] + wait)
            pr_runner_seconds += item.parallel_count * item.duration_sec
            sharded += 1
            for run in pr_runs:
                if str(run.run_id) == run_id:
                    run.rwdi_wall_sec = wall
                    break
        elif item.is_pr_rwdi:
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
) -> tuple[ScenarioResult, ScenarioResult]:
    baseline = replay(
        build_work_items(jobs, pr_runs, parallel=False, shard_eligible=shard_eligible),
        config,
        name="monolith",
        pr_runs=pr_runs,
        parallel=False,
    )
    parallel = replay(
        build_work_items(jobs, pr_runs, parallel=True, shard_eligible=shard_eligible),
        config,
        name="sharding",
        pr_runs=pr_runs,
        parallel=True,
    )
    return baseline, parallel
