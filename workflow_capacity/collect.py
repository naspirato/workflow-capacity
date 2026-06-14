#!/usr/bin/env python3
"""Collect self-hosted workflow job intervals from GitHub Actions."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_REPO = "ydb-platform/ydb"

WORKFLOW_NAMES = (
    "PR-check",
    "Postcommit_asan",
    "Postcommit_relwithdebinfo",
    "Nightly-Build",
    "Run-tests",
    "Regression-run",
    "Regression-run_Large",
    "Regression-run_Small_and_Medium",
    "Regression-run_stress",
    "Regression-run_compatibility",
    "Regression-whitelist-run",
    "Collect-analytics-run",
    "Collect-analytics-fast-run",
    "Run and debug tests",
    "Compare-ydb-configs-in-branches",
    "Update Muted tests",
    "Publish docker image",
    "Prewarm-Ccache",
)

MEANINGFUL_CONCLUSIONS = {"success", "failure", "timed_out"}
MAX_WORKERS = 6


def gh_api(path: str, *, retries: int = 8) -> Any:
    last_err = ""
    for attempt in range(retries):
        try:
            out = subprocess.check_output(
                ["gh", "api", path],
                stderr=subprocess.STDOUT,
                text=True,
            )
            return json.loads(out)
        except subprocess.CalledProcessError as exc:
            last_err = (exc.output or str(exc)).strip()
            if attempt + 1 == retries:
                raise RuntimeError(f"gh api failed for {path}: {last_err}") from exc
            time.sleep(min(30, 2 ** attempt))
    raise RuntimeError(f"gh api failed for {path}: {last_err}")


def load_workflow_ids(repo: str) -> dict[str, int]:
    data = gh_api(f"repos/{repo}/actions/workflows?per_page=100")
    by_name = {w["name"]: w["id"] for w in data["workflows"]}
    missing = [name for name in WORKFLOW_NAMES if name not in by_name]
    if missing:
        print(f"warning: workflows not found: {missing}", file=sys.stderr)
    return {name: by_name[name] for name in WORKFLOW_NAMES if name in by_name}


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def preset_label(labels: list[str]) -> str | None:
    for label in labels:
        if label.startswith("build-preset-"):
            return label
    return None


def is_auto_provisioned(labels: list[str]) -> bool:
    lowered = {x.lower() for x in labels}
    return "self-hosted" in lowered and "auto-provisioned" in lowered


def iter_runs(workflow_id: int, repo: str, since: datetime, until: datetime | None = None):
    until = until or datetime.now(timezone.utc)
    chunks: list[tuple[datetime, datetime]] = [(since, until)]
    seen_run_ids: set[int] = set()

    while chunks:
        chunk_start, chunk_end = chunks.pop(0)
        if chunk_end <= chunk_start:
            continue
        created_param = (
            f"{chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            f"..{chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        page = 1
        hit_page_cap = False
        while page <= 10:
            data = gh_api(
                f"repos/{repo}/actions/workflows/{workflow_id}/runs"
                f"?created={created_param}&per_page=100&page={page}"
            )
            runs = data.get("workflow_runs", [])
            if not runs:
                break
            for run in runs:
                rid = run["id"]
                if rid in seen_run_ids:
                    continue
                seen_run_ids.add(rid)
                yield run
            if len(runs) < 100:
                break
            if page == 10:
                hit_page_cap = True
            page += 1

        if hit_page_cap:
            span = chunk_end - chunk_start
            if span <= timedelta(minutes=30):
                print(
                    f"warning: workflow {workflow_id} chunk {created_param} still hits 1k cap",
                    file=sys.stderr,
                )
                continue
            mid = chunk_start + span / 2
            chunks.insert(0, (mid, chunk_end))
            chunks.insert(0, (chunk_start, mid))


def fetch_jobs(repo: str, run: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    page = 1
    while True:
        data = gh_api(f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100&page={page}")
        chunk = data.get("jobs", [])
        jobs.extend(chunk)
        if len(chunk) < 100:
            break
        page += 1
    return jobs


def normalize_job(
    job: dict[str, Any],
    *,
    workflow_name: str,
    run: dict[str, Any],
) -> dict[str, Any] | None:
    labels = job.get("labels") or []
    if not is_auto_provisioned(labels):
        return None
    preset = preset_label(labels)
    if not preset:
        return None
    started = parse_ts(job.get("started_at"))
    created = parse_ts(job.get("created_at"))
    completed = parse_ts(job.get("completed_at"))
    if not started or not completed or completed <= started:
        return None
    queue_wait_sec = None
    if created and started > created:
        queue_wait_sec = (started - created).total_seconds()
    pr_numbers = [pr["number"] for pr in run.get("pull_requests") or []]
    base_ref = ""
    if run.get("pull_requests"):
        base_ref = (run["pull_requests"][0].get("base") or {}).get("ref") or ""
    return {
        "job_id": job["id"],
        "run_id": run["id"],
        "workflow_name": workflow_name,
        "job_name": job.get("name") or "",
        "preset": preset,
        "status": job.get("status"),
        "conclusion": job.get("conclusion"),
        "created_at": created.isoformat() if created else None,
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "duration_sec": (completed - started).total_seconds(),
        "queue_wait_sec": queue_wait_sec,
        "runner_name": job.get("runner_name") or "",
        "pr_number": pr_numbers[0] if pr_numbers else None,
        "base_ref": base_ref,
        "run_conclusion": run.get("conclusion"),
        "head_branch": run.get("head_branch") or "",
        "run_created_at": run.get("created_at"),
    }


def collect_window(
    *,
    repo: str = DEFAULT_REPO,
    since: datetime,
    until: datetime | None = None,
    output: Path,
    workflows: list[str] | None = None,
) -> dict[str, Any]:
    until = until or datetime.now(timezone.utc)
    workflow_ids = load_workflow_ids(repo)
    if workflows:
        workflow_ids = {name: workflow_ids[name] for name in workflows if name in workflow_ids}

    jobs: list[dict[str, Any]] = []
    stats = Counter()
    pending_runs: list[tuple[str, dict[str, Any]]] = []

    for workflow_name, workflow_id in workflow_ids.items():
        print(f"listing runs: {workflow_name}", flush=True)
        for run in iter_runs(workflow_id, repo, since, until):
            stats["runs_seen"] += 1
            if run.get("status") != "completed":
                stats["runs_skipped_incomplete"] += 1
                continue
            if run.get("conclusion") not in MEANINGFUL_CONCLUSIONS:
                stats["runs_skipped_conclusion"] += 1
                continue
            pending_runs.append((workflow_name, run))

    print(f"fetching jobs for {len(pending_runs)} runs", flush=True)

    def worker(item: tuple[str, dict[str, Any]]) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
        workflow_name, run = item
        return workflow_name, run, fetch_jobs(repo, run)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker, item) for item in pending_runs]
        for idx, future in enumerate(as_completed(futures), start=1):
            workflow_name, run, run_jobs = future.result()
            stats["runs_with_jobs"] += 1
            for job in run_jobs:
                normalized = normalize_job(job, workflow_name=workflow_name, run=run)
                if normalized:
                    jobs.append(normalized)
                    stats["jobs_kept"] += 1
                else:
                    stats["jobs_dropped"] += 1
            if idx % 200 == 0:
                print(f"  processed {idx}/{len(pending_runs)} runs, jobs={len(jobs)}", flush=True)

    payload = {
        "repo": repo,
        "since": since.isoformat(),
        "until": until.isoformat(),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "workflow_names": list(workflow_ids.keys()),
        "stats": dict(stats),
        "jobs": jobs,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"wrote {len(jobs)} jobs to {output}", flush=True)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--workflows",
        default="",
        help="Comma-separated workflow names (default: all known workflows)",
    )
    args = parser.parse_args()
    until = datetime.now(timezone.utc)
    since = until - timedelta(days=args.days)
    workflows = [w.strip() for w in args.workflows.split(",") if w.strip()] or None
    collect_window(
        repo=args.repo,
        since=since,
        until=until,
        output=args.output,
        workflows=workflows,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
