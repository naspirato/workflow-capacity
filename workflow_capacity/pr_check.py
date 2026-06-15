"""Model PR-check jobs (relwithdebinfo + release-asan) under baseline vs sharding."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from workflow_capacity.pr_classify import ClassifyRules, classify_pr_number
from workflow_capacity.sharding import choose_shard_count, is_peak_hour_utc

PREPARE_MIN_SEC = 15 * 60
PREPARE_MAX_SEC = 25 * 60
PREPARE_FRACTION = 0.18
SHARD_OVERHEAD = 1.08
DEFAULT_THREADS = 52


@dataclass
class PrCheckRun:
    run_id: int
    pr_number: int | None
    rwdi_job: dict[str, Any]
    asan_job: dict[str, Any] | None
    mode: str
    shard_count: int
    rwdi_wall_sec: float


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def estimate_prepare_sec(mono_duration_sec: float) -> float:
    return min(PREPARE_MAX_SEC, max(PREPARE_MIN_SEC, mono_duration_sec * PREPARE_FRACTION))


def estimate_shard_count(
    mono_duration_sec: float,
    *,
    started_at: datetime,
    capacity_cap: int,
) -> tuple[int, float]:
    prepare = estimate_prepare_sec(mono_duration_sec)
    test_sec = max(mono_duration_sec - prepare, 60.0)
    hour = started_at.astimezone(__import__("datetime").timezone.utc).hour
    peak = is_peak_hour_utc(hour)
    count, estimate_min = choose_shard_count(
        test_sec * DEFAULT_THREADS,
        threads=DEFAULT_THREADS,
        is_peak=peak,
        max_shards=capacity_cap if capacity_cap > 0 else 0,
    )
    return count, estimate_min


def sharded_rwdi_timeline(
    mono_duration_sec: float,
    *,
    started_at: datetime,
    capacity_cap: int,
) -> tuple[float, int, float, float]:
    """Return wall-clock, shard_count, prepare_sec, shard_sec for a PR-check build job."""
    prepare_sec = estimate_prepare_sec(mono_duration_sec)
    test_sec = max(mono_duration_sec - prepare_sec, 60.0)
    shard_count, _ = estimate_shard_count(
        mono_duration_sec,
        started_at=started_at,
        capacity_cap=capacity_cap,
    )
    shard_sec = (test_sec / shard_count) * SHARD_OVERHEAD
    wall = prepare_sec + shard_sec
    return wall, shard_count, prepare_sec, shard_sec


def parse_pr_shard_key(key: str) -> tuple[str, int] | None:
    """Parse ``prepare:BRANCH:run_id`` or ``shards:BRANCH:run_id`` → (branch, run_id)."""
    parts = key.split(":")
    if len(parts) == 3 and parts[0] in ("prepare", "shards") and parts[1] in ("rwdi", "asan"):
        return parts[1], int(parts[2])
    return None


def build_pr_check_runs(
    jobs: list[dict[str, Any]],
    *,
    classify: bool = True,
    repo: str = "ydb-platform/ydb",
    pr_files: dict[str, Any] | None = None,
    classify_rules: ClassifyRules | None = None,
    fetch_if_missing: bool = False,
) -> list[PrCheckRun]:
    by_run: dict[int, dict[str, Any]] = {}
    for job in jobs:
        if job["workflow_name"] != "PR-check":
            continue
        bucket = by_run.setdefault(job["run_id"], {"rwdi": None, "asan": None, "pr": job.get("pr_number")})
        name = job["job_name"]
        if "relwithdebinfo" in name:
            bucket["rwdi"] = job
        elif "release-asan" in name or "asan" in name:
            bucket["asan"] = job

    rules = classify_rules or ClassifyRules.default()
    pr_mode_cache: dict[int, str] = {}
    runs: list[PrCheckRun] = []
    for run_id, bucket in by_run.items():
        rwdi = bucket["rwdi"]
        if not rwdi:
            continue
        pr_number = bucket["pr"] or rwdi.get("pr_number")
        mode = "sharded"
        if pr_number:
            if pr_number not in pr_mode_cache:
                if classify:
                    mode_val, _ = classify_pr_number(
                        int(pr_number),
                        repo=repo,
                        pr_files=pr_files,
                        rules=rules,
                        fetch_if_missing=fetch_if_missing,
                    )
                    pr_mode_cache[int(pr_number)] = mode_val
                else:
                    pr_mode_cache[int(pr_number)] = "sharded"
            mode = pr_mode_cache[int(pr_number)]
        elif classify:
            mode = "single"
        else:
            mode = "sharded"
        mono = float(rwdi["duration_sec"])
        started = parse_ts(rwdi["started_at"])
        shard_count = 1
        wall = mono
        if mode == "sharded":
            wall, shard_count, _, _ = sharded_rwdi_timeline(
                mono, started_at=started, capacity_cap=12
            )
        runs.append(
            PrCheckRun(
                run_id=run_id,
                pr_number=pr_number,
                rwdi_job=rwdi,
                asan_job=bucket["asan"],
                mode=mode,
                shard_count=shard_count,
                rwdi_wall_sec=wall,
            )
        )
    return runs
