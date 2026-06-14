"""Cached historical job datasets."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from workflow_capacity.log import status

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CACHE_DIR = ROOT / "data" / "cache"


@dataclass
class JobsDataset:
    path: Path
    repo: str
    since: str
    until: str
    jobs: list[dict[str, Any]]
    stats: dict[str, Any]

    @classmethod
    def load(cls, path: Path) -> JobsDataset:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            path=path,
            repo=payload.get("repo", ""),
            since=payload.get("since", ""),
            until=payload.get("until", payload.get("since", "")),
            jobs=payload["jobs"],
            stats=payload.get("stats", {}),
        )


def cache_path(
    cache_dir: Path,
    repo: str,
    since: datetime,
    until: datetime,
) -> Path:
    slug = repo.replace("/", "_")
    return cache_dir / f"jobs_{slug}_{since.date()}_{until.date()}.json"


def list_datasets(cache_dir: Path = DEFAULT_CACHE_DIR) -> list[JobsDataset]:
    if not cache_dir.exists():
        return []
    out: list[JobsDataset] = []
    for path in sorted(cache_dir.glob("jobs_*.json")):
        try:
            out.append(JobsDataset.load(path))
        except (json.JSONDecodeError, KeyError):
            continue
    return out


def load_dataset(path: Path | str) -> JobsDataset:
    return JobsDataset.load(Path(path))


def ensure_dataset(
    *,
    days: int | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    repo: str = "ydb-platform/ydb",
    cache_dir: Path = DEFAULT_CACHE_DIR,
    refresh: bool = False,
    augment: bool = True,
) -> JobsDataset:
    """Load from cache or collect from GitHub once per date window."""
    until = until or datetime.now(timezone.utc)
    since = since or (until - timedelta(days=days or 14))
    path = cache_path(cache_dir, repo, since, until)
    if path.exists() and not refresh:
        try:
            dataset = JobsDataset.load(path)
            status(
                f"cache: loaded {path.name} "
                f"({len(dataset.jobs)} jobs, {dataset.since[:10]} .. {dataset.until[:10]})"
            )
            return dataset
        except (json.JSONDecodeError, KeyError):
            status(f"cache: corrupt file {path.name}, re-collecting from GitHub ...")
            path.unlink(missing_ok=True)

    from workflow_capacity.collect import collect_window

    status(
        f"cache: downloading {path.name} "
        f"({since.date()} .. {until.date()}, refresh={refresh}) ..."
    )
    payload = collect_window(repo=repo, since=since, until=until, output=path)
    dataset = JobsDataset(
        path=path,
        repo=payload["repo"],
        since=payload["since"],
        until=payload["until"],
        jobs=payload["jobs"],
        stats=payload.get("stats", {}),
    )
    if augment:
        from workflow_capacity.augment import augment_file

        status(f"cache: augmenting PR metadata in {path.name} ...")
        augment_file(path, repo=repo)
        dataset = JobsDataset.load(path)
    status(f"cache: ready — {len(dataset.jobs)} jobs in {path.name}")
    return dataset
