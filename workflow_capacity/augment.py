#!/usr/bin/env python3
"""Resolve PR target branch (base_ref) from GitHub Pull Request API, not workflow runs."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from workflow_capacity.log import status

REPO = "ydb-platform/ydb"
REPO_OWNER, REPO_NAME = REPO.split("/", 1)

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "data" / "jobs_14d.json"

MUTED_RE = re.compile(
    r"^update-muted-ya_(.+?)_(?:relwithdebinfo|release-asan|release-tsan|release-msan)$"
)
MERGE_STABLE_RE = re.compile(r"^(stable-[\w-]+)-merge-[0-9a-f]+$")
CHERRY_STABLE_RE = re.compile(r"^cherry-pick-(stable-[\w-]+)-")

GQL_HEAD_REF = """
query($owner: String!, $name: String!, $headRefName: String!) {
  repository(owner: $owner, name: $name) {
    pullRequests(
      states: [OPEN, MERGED, CLOSED]
      headRefName: $headRefName
      first: 1
      orderBy: {field: UPDATED_AT, direction: DESC}
    ) {
      nodes {
        number
        baseRefName
      }
    }
  }
}
"""


@dataclass
class PrMeta:
    pr_number: int | None
    base_ref: str
    source: str


def gh_json(path: str, *, headers: list[str] | None = None) -> object:
    cmd = ["gh", "api", path]
    for header in headers or []:
        cmd.extend(["-H", header])
    out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    return json.loads(out)


def fetch_pr_by_number(pr_number: int) -> PrMeta:
    try:
        data = gh_json(f"repos/{REPO}/pulls/{pr_number}")
        return PrMeta(
            pr_number=int(data["number"]),
            base_ref=str(data["base"]["ref"]),
            source="pr_number",
        )
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError):
        return PrMeta(None, "", "missing")


def fetch_pr_by_head_same_repo(head_branch: str) -> PrMeta:
    if not head_branch:
        return PrMeta(None, "", "missing")
    try:
        data = gh_json(
            f"repos/{REPO}/pulls"
            f"?head={REPO_OWNER}:{head_branch}&state=all&per_page=1"
        )
        if not data:
            return PrMeta(None, "", "missing")
        pr = data[0]
        return PrMeta(
            pr_number=int(pr["number"]),
            base_ref=str(pr["base"]["ref"]),
            source="head_lookup",
        )
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError, IndexError):
        return PrMeta(None, "", "missing")


def fetch_pr_by_head_ref_name(head_branch: str) -> PrMeta:
    """Match PRs from forks via head ref name (pull_request_target runs)."""
    if not head_branch:
        return PrMeta(None, "", "missing")
    try:
        out = subprocess.check_output(
            [
                "gh",
                "api",
                "graphql",
                "-f",
                f"query={GQL_HEAD_REF}",
                "-f",
                f"owner={REPO_OWNER}",
                "-f",
                f"name={REPO_NAME}",
                "-f",
                f"headRefName={head_branch}",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        nodes = json.loads(out)["data"]["repository"]["pullRequests"]["nodes"]
        if not nodes:
            return PrMeta(None, "", "missing")
        pr = nodes[0]
        return PrMeta(
            pr_number=int(pr["number"]),
            base_ref=str(pr["baseRefName"]),
            source="head_ref_graphql",
        )
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError, IndexError):
        return PrMeta(None, "", "missing")


def fetch_pr_by_commit_sha(head_sha: str) -> PrMeta:
    if not head_sha:
        return PrMeta(None, "", "missing")
    try:
        data = gh_json(
            f"repos/{REPO}/commits/{head_sha}/pulls",
            headers=["Accept: application/vnd.github.groot-preview+json"],
        )
        if not data:
            return PrMeta(None, "", "missing")
        pr = data[0]
        return PrMeta(
            pr_number=int(pr["number"]),
            base_ref=str(pr["base"]["ref"]),
            source="commit_sha",
        )
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError, IndexError):
        return PrMeta(None, "", "missing")


def infer_base_ref(head_branch: str) -> str:
    """Fallback when no PR exists in GitHub (bots, muted updates, merge branches)."""
    head = head_branch or ""
    if not head:
        return ""

    muted = MUTED_RE.match(head)
    if muted:
        return muted.group(1)

    merge = MERGE_STABLE_RE.match(head)
    if merge:
        return merge.group(1)

    cherry = CHERRY_STABLE_RE.match(head)
    if cherry:
        return cherry.group(1)

    if head in {"main", "master"}:
        return "main"

    if head.startswith(("merge-main-", "merge-rightlib-")):
        return "main"

    if head.startswith("stable-") and "-merge-" not in head:
        return head.split("_")[0]

    return ""


def resolve_head_branch(head_branch: str, head_cache: dict[str, PrMeta]) -> PrMeta:
    if head_branch in head_cache:
        return head_cache[head_branch]

    for fetcher, delay in (
        (fetch_pr_by_head_same_repo, 0.03),
        (fetch_pr_by_head_ref_name, 0.04),
    ):
        meta = fetcher(head_branch)
        time.sleep(delay)
        if meta.base_ref:
            head_cache[head_branch] = meta
            return meta

    head_cache[head_branch] = PrMeta(None, "", "missing")
    return head_cache[head_branch]


def resolve_run_meta(
    *,
    pr_number: int | None,
    head_branch: str,
    head_sha: str,
    pr_cache: dict[int, PrMeta],
    head_cache: dict[str, PrMeta],
    sha_cache: dict[str, PrMeta],
) -> PrMeta:
    if pr_number:
        if pr_number not in pr_cache:
            pr_cache[pr_number] = fetch_pr_by_number(pr_number)
            time.sleep(0.03)
        meta = pr_cache[pr_number]
        if meta.base_ref:
            return meta

    head = head_branch or ""
    if head:
        meta = resolve_head_branch(head, head_cache)
        if meta.base_ref:
            return meta

    sha = head_sha or ""
    if sha:
        if sha not in sha_cache:
            sha_cache[sha] = fetch_pr_by_commit_sha(sha)
            time.sleep(0.03)
        meta = sha_cache[sha]
        if meta.base_ref:
            return meta

    inferred = infer_base_ref(head)
    if inferred:
        return PrMeta(pr_number, inferred, "inferred")

    return PrMeta(pr_number, "", "missing")


def fetch_run_head_sha(run_id: int) -> str:
    try:
        data = gh_json(f"repos/{REPO}/actions/runs/{run_id}")
        return str(data.get("head_sha") or "")
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError):
        return ""


def augment(path: Path, *, resolve_run_sha: bool) -> dict[str, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    jobs = payload.get("jobs", [])

    run_info: dict[int, dict[str, object]] = {}
    for job in jobs:
        if job.get("workflow_name") != "PR-check":
            continue
        rid = int(job["run_id"])
        info = run_info.setdefault(
            rid,
            {
                "head_branch": job.get("head_branch") or "",
                "pr_number": job.get("pr_number"),
                "head_sha": job.get("head_sha") or "",
            },
        )
        if job.get("pr_number") and not info.get("pr_number"):
            info["pr_number"] = job["pr_number"]
        if job.get("head_branch") and not info.get("head_branch"):
            info["head_branch"] = job["head_branch"]
        if job.get("head_sha") and not info.get("head_sha"):
            info["head_sha"] = job["head_sha"]

    pr_cache: dict[int, PrMeta] = {}
    head_cache: dict[str, PrMeta] = {}
    sha_cache: dict[str, PrMeta] = {}
    run_meta: dict[int, PrMeta] = {}
    sources: dict[str, int] = {}
    total_runs = len(run_info)
    status(f"augment: resolving base_ref for {total_runs} PR-check runs ...")

    for idx, (rid, info) in enumerate(run_info.items(), 1):
        meta = resolve_run_meta(
            pr_number=info.get("pr_number"),  # type: ignore[arg-type]
            head_branch=str(info.get("head_branch") or ""),
            head_sha=str(info.get("head_sha") or ""),
            pr_cache=pr_cache,
            head_cache=head_cache,
            sha_cache=sha_cache,
        )
        run_meta[rid] = meta
        sources[meta.source] = sources.get(meta.source, 0) + 1
        if idx % 100 == 0 or idx == total_runs:
            status(f"  augment: {idx}/{total_runs} runs resolved ...")

    if resolve_run_sha:
        missing_rids = [rid for rid, meta in run_meta.items() if not meta.base_ref]
        for rid in missing_rids:
            head_sha = fetch_run_head_sha(rid)
            time.sleep(0.03)
            if not head_sha:
                continue
            run_info[rid]["head_sha"] = head_sha
            meta = resolve_run_meta(
                pr_number=run_info[rid].get("pr_number"),  # type: ignore[arg-type]
                head_branch=str(run_info[rid].get("head_branch") or ""),
                head_sha=head_sha,
                pr_cache=pr_cache,
                head_cache=head_cache,
                sha_cache=sha_cache,
            )
            if meta.base_ref:
                old = run_meta[rid]
                run_meta[rid] = meta
                sources[old.source] -= 1
                if sources[old.source] <= 0:
                    sources.pop(old.source, None)
                sources[meta.source] = sources.get(meta.source, 0) + 1

    updated_jobs = 0
    for job in jobs:
        rid = int(job["run_id"])
        meta = run_meta.get(rid)
        if not meta:
            continue
        changed = False
        if meta.pr_number and job.get("pr_number") != meta.pr_number:
            job["pr_number"] = meta.pr_number
            changed = True
        if meta.base_ref and job.get("base_ref") != meta.base_ref:
            job["base_ref"] = meta.base_ref
            changed = True
        if meta.base_ref:
            job["base_ref_source"] = meta.source
        run_sha = run_info.get(rid, {}).get("head_sha")
        if run_sha and job.get("head_sha") != run_sha:
            job["head_sha"] = run_sha
            changed = True
        if changed:
            updated_jobs += 1

    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    stats = {
        "pr_check_runs": len(run_info),
        "prs_cached": len(pr_cache),
        "heads_cached": len(head_cache),
        "shas_cached": len(sha_cache),
        "jobs_updated": updated_jobs,
        **sources,
    }
    status(
        f"augment: done — {updated_jobs} jobs updated, "
        f"sources: {', '.join(f'{k}={v}' for k, v in sorted(sources.items()))}"
    )
    return stats


def augment_file(
    path: Path,
    *,
    repo: str = "ydb-platform/ydb",
    resolve_run_sha: bool = False,
) -> dict[str, int]:
    global REPO, REPO_OWNER, REPO_NAME
    REPO = repo
    REPO_OWNER, REPO_NAME = repo.split("/", 1)
    return augment(path, resolve_run_sha=resolve_run_sha)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument(
        "--resolve-run-sha",
        action="store_true",
        help="For still-missing runs, fetch workflow run head_sha and retry commit lookup",
    )
    args = parser.parse_args()
    stats = augment(args.data, resolve_run_sha=args.resolve_run_sha)
    print(json.dumps(stats, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
