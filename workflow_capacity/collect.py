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
import threading
import time
from collections import Counter
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

# Excluded from collect — add here only if a workflow should not compete in pool replay.
WORKFLOW_BLACKLIST = frozenset()

MEANINGFUL_CONCLUSIONS = ("success", "failure", "timed_out")
GH_API_TIMEOUT_SEC = 120
JOB_FETCH_HEARTBEAT_SEC = 30
JOB_FETCH_INTERVAL_SEC = 0.85
CHECKPOINT_EVERY_RUNS = 100
_RATE_LIMIT_BUFFER_SEC = 10
_rate_limit_lock = threading.Lock()
_request_throttle_lock = threading.Lock()
_last_request_at = 0.0

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


def _is_rate_limit_error(message: str) -> bool:
    return "rate limit" in message.lower()


def _run_gh(path: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [gh_executable(), "api", path],
        capture_output=True,
        text=True,
        check=True,
        env=_gh_subprocess_env(),
        timeout=GH_API_TIMEOUT_SEC,
    )


def _throttle_request() -> None:
    global _last_request_at
    with _request_throttle_lock:
        wait = JOB_FETCH_INTERVAL_SEC - (time.monotonic() - _last_request_at)
        if wait > 0:
            time.sleep(wait)
        _last_request_at = time.monotonic()


def wait_for_github_rate_limit(*, min_remaining: int = 200) -> None:
    """Wait until core REST quota recovers (GET /rate_limit does not consume quota)."""
    with _rate_limit_lock:
        proc = _run_gh("rate_limit")
        data = _parse_gh_json(proc.stdout)
        resources = data.get("resources") or {}
        for key in ("core", "graphql"):
            bucket = resources.get(key) or {}
            remaining = int(bucket.get("remaining") or 0)
            reset = int(bucket.get("reset") or 0)
            if remaining >= min_remaining:
                continue
            wait_sec = max(0.0, reset - time.time() + _RATE_LIMIT_BUFFER_SEC)
            reset_at = datetime.fromtimestamp(reset, tz=timezone.utc)
            status(
                f"collect: GitHub {key} rate limit ({remaining}/{bucket.get('limit', '?')} left) — "
                f"waiting {wait_sec:.0f}s until {reset_at:%H:%M:%S} UTC ..."
            )
            time.sleep(wait_sec)
            return


def gh_api(path: str, *, retries: int = 8, throttle: bool = False) -> Any:
    last_err = ""
    attempt = 0
    while True:
        if throttle:
            _throttle_request()
        try:
            proc = _run_gh(path)
        except subprocess.TimeoutExpired as exc:
            attempt += 1
            last_err = f"timed out after {GH_API_TIMEOUT_SEC}s"
            if attempt >= retries:
                raise GhApiError(f"gh api {path!r} failed: {last_err}") from exc
            time.sleep(min(30, 2 ** (attempt - 1)))
            continue
        except subprocess.CalledProcessError as exc:
            last_err = (exc.stderr or exc.stdout or "").strip()
            if _is_rate_limit_error(last_err):
                wait_for_github_rate_limit(min_remaining=200)
                continue
            attempt += 1
            if attempt >= retries:
                lowered = last_err.lower()
                if "auth login" in lowered or "gh_token" in lowered:
                    raise GhApiError(
                        "GitHub CLI is not authenticated in this environment. "
                        "Run `gh auth login` in a terminal, then restart the notebook kernel."
                    ) from exc
                raise GhApiError(f"gh api {path!r} failed: {last_err}") from exc
            time.sleep(min(30, 2 ** (attempt - 1)))
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


def load_workflow_ids(repo: str) -> dict[str, int]:
    data = gh_api(f"repos/{repo}/actions/workflows?per_page=100")
    by_name = {w["name"]: w["id"] for w in data["workflows"]}
    missing = [
        name
        for name in WORKFLOW_NAMES
        if name not in by_name and name not in WORKFLOW_BLACKLIST
    ]
    if missing:
        print(f"warning: workflows not found: {missing}", file=sys.stderr)
    blacklisted = sorted(name for name in WORKFLOW_BLACKLIST if name in by_name)
    if blacklisted:
        status(f"collect: skipping blacklisted workflows: {', '.join(blacklisted)}")
    return {
        name: by_name[name]
        for name in WORKFLOW_NAMES
        if name in by_name and name not in WORKFLOW_BLACKLIST
    }


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


def iter_repo_runs(
    repo: str,
    since: datetime,
    until: datetime | None,
    *,
    allowed_names: set[str],
    allowed_ids: set[int],
):
    """List runs repo-wide (one paginated stream per conclusion, not per workflow)."""
    until = until or datetime.now(timezone.utc)
    seen_run_ids: set[int] = set()

    for conclusion in MEANINGFUL_CONCLUSIONS:
        chunks: list[tuple[datetime, datetime]] = [(since, until)]
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
                    f"repos/{repo}/actions/runs"
                    f"?created={created_param}&per_page=100&page={page}&status={conclusion}"
                )
                runs = data.get("workflow_runs", [])
                if not runs:
                    break
                for run in runs:
                    rid = run["id"]
                    if rid in seen_run_ids:
                        continue
                    seen_run_ids.add(rid)
                    wf_id = run.get("workflow_id")
                    wf_name = run.get("name") or ""
                    if wf_id not in allowed_ids and wf_name not in allowed_names:
                        continue
                    yield wf_name, run
                if len(runs) < 100:
                    break
                if page == 10:
                    hit_page_cap = True
                page += 1

            if hit_page_cap:
                span = chunk_end - chunk_start
                if span <= timedelta(minutes=30):
                    print(
                        f"warning: repo runs chunk {created_param} "
                        f"status={conclusion} still hits 1k cap",
                        file=sys.stderr,
                    )
                    continue
                mid = chunk_start + span / 2
                chunks.insert(0, (mid, chunk_end))
                chunks.insert(0, (chunk_start, mid))


def iter_runs(
    workflow_id: int,
    repo: str,
    since: datetime,
    until: datetime | None = None,
    *,
    conclusions: tuple[str, ...] = MEANINGFUL_CONCLUSIONS,
):
    """Yield completed workflow runs with meaningful conclusions (excludes skipped/cancelled)."""
    until = until or datetime.now(timezone.utc)
    seen_run_ids: set[int] = set()

    for conclusion in conclusions:
        chunks: list[tuple[datetime, datetime]] = [(since, until)]
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
                    f"?created={created_param}&per_page=100&page={page}&status={conclusion}"
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
                        f"warning: workflow {workflow_id} chunk {created_param} "
                        f"status={conclusion} still hits 1k cap",
                        file=sys.stderr,
                    )
                    continue
                mid = chunk_start + span / 2
                chunks.insert(0, (mid, chunk_end))
                chunks.insert(0, (chunk_start, mid))


def _checkpoint_path(output: Path) -> Path:
    return output.with_name(output.name + ".partial.json")


def _load_checkpoint(output: Path, *, repo: str, since: datetime, until: datetime) -> dict[str, Any] | None:
    path = _checkpoint_path(output)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if (
        data.get("repo") != repo
        or data.get("since") != since.isoformat()
        or data.get("until") != until.isoformat()
    ):
        return None
    return data


def _save_checkpoint(
    output: Path,
    *,
    repo: str,
    since: datetime,
    until: datetime,
    workflow_names: list[str],
    jobs: list[dict[str, Any]],
    stats: dict[str, Any],
    completed_run_ids: list[int],
) -> None:
    path = _checkpoint_path(output)
    path.write_text(
        json.dumps(
            {
                "repo": repo,
                "since": since.isoformat(),
                "until": until.isoformat(),
                "workflow_names": workflow_names,
                "completed_run_ids": completed_run_ids,
                "stats": stats,
                "jobs": jobs,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def fetch_jobs(repo: str, run: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    page = 1
    while True:
        data = gh_api(
            f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100&page={page}",
            throttle=True,
        )
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

    allowed_names = set(workflow_ids.keys())
    allowed_ids = set(workflow_ids.values())
    jobs: list[dict[str, Any]] = []
    stats: Counter = Counter()
    completed_ids: set[int] = set()

    checkpoint = _load_checkpoint(output, repo=repo, since=since, until=until)
    if checkpoint:
        jobs = list(checkpoint.get("jobs") or [])
        stats = Counter(checkpoint.get("stats") or {})
        completed_ids = set(checkpoint.get("completed_run_ids") or [])
        status(
            f"collect: resume checkpoint — {len(completed_ids)} runs done, "
            f"{len(jobs)} jobs kept"
        )

    status("collect: listing runs (repo-wide, throttled job fetch) ...")
    pending_runs: list[tuple[str, dict[str, Any]]] = []
    listed = 0

    for workflow_name, run in iter_repo_runs(
        repo,
        since,
        until,
        allowed_names=allowed_names,
        allowed_ids=allowed_ids,
    ):
        stats["runs_seen"] += 1
        listed += 1
        if listed % 500 == 0:
            status(f"  listing: {listed} matching runs ...")
        if run.get("status") != "completed":
            stats["runs_skipped_incomplete"] += 1
            continue
        conclusion = run.get("conclusion")
        if conclusion not in MEANINGFUL_CONCLUSIONS:
            stats["runs_skipped_conclusion"] += 1
            continue
        if run["id"] in completed_ids:
            continue
        pending_runs.append((workflow_name, run))

    status(
        f"collect: {listed} runs listed, {len(pending_runs)} to fetch "
        f"({len(completed_ids)} already in checkpoint)"
    )

    total_runs = len(completed_ids) + len(pending_runs)
    wait_for_github_rate_limit(min_remaining=500)
    status(
        f"collect: fetching jobs for {len(pending_runs)} runs "
        f"(~{JOB_FETCH_INTERVAL_SEC:.1f}s/request, est. "
        f"{len(pending_runs) * JOB_FETCH_INTERVAL_SEC / 60:.0f} min) ..."
    )

    done_count = len(completed_ids)
    completed_run_ids = sorted(completed_ids)
    last_heartbeat = time.monotonic()

    for idx, (workflow_name, run) in enumerate(pending_runs, start=1):
        run_jobs = fetch_jobs(repo, run)
        stats["runs_with_jobs"] += 1
        done_count += 1
        completed_ids.add(run["id"])
        completed_run_ids.append(run["id"])
        for job in run_jobs:
            normalized = normalize_job(job, workflow_name=workflow_name, run=run)
            if normalized:
                jobs.append(normalized)
                stats["jobs_kept"] += 1
            else:
                stats["jobs_dropped"] += 1

        now = time.monotonic()
        if (
            idx % 50 == 0
            or idx == len(pending_runs)
            or now - last_heartbeat >= JOB_FETCH_HEARTBEAT_SEC
        ):
            status(
                f"  jobs: {done_count}/{total_runs} runs, "
                f"{len(jobs)} kept ({stats['jobs_dropped']} dropped)"
            )
            last_heartbeat = now

        if idx % CHECKPOINT_EVERY_RUNS == 0 or idx == len(pending_runs):
            _save_checkpoint(
                output,
                repo=repo,
                since=since,
                until=until,
                workflow_names=list(workflow_ids.keys()),
                jobs=jobs,
                stats=dict(stats),
                completed_run_ids=completed_run_ids,
            )

    checkpoint_file = _checkpoint_path(output)
    if checkpoint_file.exists():
        checkpoint_file.unlink()

    payload = {
        "repo": repo,
        "since": since.isoformat(),
        "until": until.isoformat(),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "workflow_names": list(workflow_ids.keys()),
        "workflow_blacklist": sorted(WORKFLOW_BLACKLIST),
        "meaningful_conclusions": list(MEANINGFUL_CONCLUSIONS),
        "stats": dict(stats),
        "jobs": jobs,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    status(f"collect: wrote {len(jobs)} jobs to {output.name}")
    return payload


def main() -> int:
    from workflow_capacity.cache import DEFAULT_CACHE_DIR, cache_path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON path (default: data/cache/jobs_<repo>_<since>_<until>.json)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help="Directory for default output path",
    )
    parser.add_argument(
        "--workflows",
        default="",
        help="Comma-separated workflow names (default: all known workflows)",
    )
    args = parser.parse_args()
    until = datetime.now(timezone.utc)
    since = until - timedelta(days=args.days)
    output = args.output or cache_path(args.cache_dir, args.repo, since, until)
    workflows = [w.strip() for w in args.workflows.split(",") if w.strip()] or None
    collect_window(
        repo=args.repo,
        since=since,
        until=until,
        output=output,
        workflows=workflows,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
