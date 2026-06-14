#!/usr/bin/env python3
"""Collect self-hosted workflow job intervals from GitHub Actions."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from workflow_capacity.log import status

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

_GH_CANDIDATES = ("gh", "/opt/homebrew/bin/gh", "/usr/local/bin/gh")
_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


class GhApiError(RuntimeError):
    """GitHub CLI call failed."""


def gh_executable() -> str:
    for candidate in _GH_CANDIDATES:
        if candidate == "gh":
            found = shutil.which("gh")
            if found:
                return found
            continue
        if Path(candidate).is_file():
            return candidate
    raise GhApiError(
        "GitHub CLI (`gh`) not found. Install it (`brew install gh`) and restart the notebook kernel."
    )


def _gh_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env["NO_COLOR"] = "1"
    env["CLICOLOR"] = "0"
    for key in ("GH_FORCE_TTY", "FORCE_COLOR", "CLICOLOR_FORCE"):
        env.pop(key, None)
    return env


def _parse_gh_json(out: str) -> Any:
    cleaned = _ANSI_ESCAPE_RE.sub("", out.strip())
    return json.loads(cleaned)


def gh_api(path: str, *, retries: int = 8) -> Any:
    gh = gh_executable()
    last_err = ""
    for attempt in range(retries):
        try:
            proc = subprocess.run(
                [gh, "api", path],
                capture_output=True,
                text=True,
                check=True,
                env=_gh_subprocess_env(),
            )
        except subprocess.CalledProcessError as exc:
            last_err = (exc.stderr or exc.stdout or "").strip()
            if attempt + 1 == retries:
                lowered = last_err.lower()
                if "auth login" in lowered or "gh_token" in lowered:
                    raise GhApiError(
                        "GitHub CLI is not authenticated in this environment. "
                        "Run `gh auth login` in a terminal, then restart the notebook kernel."
                    ) from exc
                raise GhApiError(f"gh api {path!r} failed: {last_err}") from exc
            time.sleep(min(30, 2 ** attempt))
            continue

        out = proc.stdout.strip()
        if not out:
            detail = proc.stderr.strip()
            raise GhApiError(
                f"gh api {path!r} returned an empty response"
                + (f": {detail}" if detail else "")
            )
        try:
            return _parse_gh_json(out)
        except json.JSONDecodeError as exc:
            raise GhApiError(
                f"gh api {path!r} returned non-JSON: {out[:200]!r}"
            ) from exc
    raise RuntimeError("unreachable")


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
    status(
        f"collect: {repo} {since.date()} .. {until.date()} → {output.name}"
    )
    status("collect: loading workflow list from GitHub ...")
    workflow_ids = load_workflow_ids(repo)
    if workflows:
        workflow_ids = {name: workflow_ids[name] for name in workflows if name in workflow_ids}
    status(f"collect: {len(workflow_ids)} workflows, scanning runs ...")

    jobs: list[dict[str, Any]] = []
    stats = Counter()
    pending_runs: list[tuple[str, dict[str, Any]]] = []
    workflow_total = len(workflow_ids)

    for wf_idx, (workflow_name, workflow_id) in enumerate(workflow_ids.items(), 1):
        status(f"[collect {wf_idx}/{workflow_total}] listing runs: {workflow_name} ...")
        wf_seen = 0
        wf_eligible = 0
        for run in iter_runs(workflow_id, repo, since, until):
            stats["runs_seen"] += 1
            wf_seen += 1
            if wf_seen % 500 == 0:
                status(f"  {workflow_name}: {wf_seen} runs scanned ...")
            if run.get("status") != "completed":
                stats["runs_skipped_incomplete"] += 1
                continue
            if run.get("conclusion") not in MEANINGFUL_CONCLUSIONS:
                stats["runs_skipped_conclusion"] += 1
                continue
            pending_runs.append((workflow_name, run))
            wf_eligible += 1
        status(
            f"[collect {wf_idx}/{workflow_total}] {workflow_name}: "
            f"{wf_eligible} eligible runs ({wf_seen} scanned)"
        )

    status(f"collect: fetching jobs for {len(pending_runs)} runs ...")

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
            if idx % 50 == 0 or idx == len(pending_runs):
                status(
                    f"  jobs: {idx}/{len(pending_runs)} runs, "
                    f"{len(jobs)} kept ({stats['jobs_dropped']} dropped)"
                )

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
    status(f"collect: wrote {len(jobs)} jobs to {output.name}")
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
