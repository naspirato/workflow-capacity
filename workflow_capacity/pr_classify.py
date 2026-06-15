"""PR path classifier: fetch changed files once, apply rules at simulation time."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from workflow_capacity.collect import gh_api

DEFAULT_FILE_COUNT_THRESHOLD = 500
DEFAULT_HEAVY_PREFIXES = (
    "ydb/core/",
    "ydb/library/",
    "ydb/public/",
    "ydb/services/",
    "ydb/apps/",
    "yql/",
    "util/",
    "library/",
    "contrib/",
    "build/",
    "devtools/",
)
DEFAULT_HEAVY_EXACT = ("ya.make", "ydb/ya.make")


@dataclass
class ClassifyRules:
    file_count_threshold: int = DEFAULT_FILE_COUNT_THRESHOLD
    heavy_path_prefixes: tuple[str, ...] = DEFAULT_HEAVY_PREFIXES
    heavy_path_exact: tuple[str, ...] = DEFAULT_HEAVY_EXACT

    @classmethod
    def default(cls) -> ClassifyRules:
        return cls()

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> ClassifyRules:
        if not raw:
            return cls.default()
        return cls(
            file_count_threshold=int(raw.get("file_count_threshold", DEFAULT_FILE_COUNT_THRESHOLD)),
            heavy_path_prefixes=tuple(raw.get("heavy_path_prefixes", DEFAULT_HEAVY_PREFIXES)),
            heavy_path_exact=tuple(raw.get("heavy_path_exact", DEFAULT_HEAVY_EXACT)),
        )

    @classmethod
    def load(cls, config_path: Path | str) -> ClassifyRules:
        raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
        return cls.from_dict(raw.get("pr_classify") if isinstance(raw, dict) else None)


@dataclass
class PrFilesSnapshot:
    file_count: int
    files: list[str]
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_count": self.file_count,
            "files": self.files,
            "truncated": self.truncated,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> PrFilesSnapshot:
        return cls(
            file_count=int(raw.get("file_count", 0)),
            files=list(raw.get("files") or []),
            truncated=bool(raw.get("truncated", False)),
        )


def fetch_pr_files_snapshot(pr_number: int, *, repo: str, max_pages: int = 10) -> PrFilesSnapshot:
    """Download PR changed-file paths from GitHub (paginated)."""
    files: list[str] = []
    truncated = False
    for page in range(1, max_pages + 1):
        try:
            chunk = gh_api(
                f"repos/{repo}/pulls/{pr_number}/files?per_page=100&page={page}"
            )
        except Exception:
            break
        if not isinstance(chunk, list) or not chunk:
            break
        files.extend(str(item["filename"]) for item in chunk if item.get("filename"))
        if len(chunk) < 100:
            break
        if page == max_pages:
            truncated = True
    return PrFilesSnapshot(file_count=len(files), files=files, truncated=truncated)


def classify_snapshot(snapshot: PrFilesSnapshot | dict[str, Any], rules: ClassifyRules | None = None) -> str:
    """Return ``sharded`` or ``single`` from cached raw file list + rules."""
    rules = rules or ClassifyRules.default()
    if isinstance(snapshot, dict):
        snapshot = PrFilesSnapshot.from_dict(snapshot)
    if snapshot.file_count >= rules.file_count_threshold:
        return "sharded"
    for path in snapshot.files:
        if path in rules.heavy_path_exact:
            return "sharded"
        for prefix in rules.heavy_path_prefixes:
            if path.startswith(prefix):
                return "sharded"
    return "single"


def classify_pr_number(
    pr_number: int,
    *,
    repo: str,
    pr_files: dict[str, Any] | None = None,
    rules: ClassifyRules | None = None,
    fetch_if_missing: bool = False,
) -> tuple[str, PrFilesSnapshot | None]:
    """Classify PR mode; prefer cache, optionally fetch from GitHub."""
    rules = rules or ClassifyRules.default()
    key = str(pr_number)
    cached = (pr_files or {}).get(key)
    if cached is not None:
        return classify_snapshot(cached, rules), PrFilesSnapshot.from_dict(cached)
    if not fetch_if_missing:
        return "single", None
    snap = fetch_pr_files_snapshot(pr_number, repo=repo)
    return classify_snapshot(snap, rules), snap


def parse_pr_files_index(raw: dict[str, Any] | None) -> dict[int, dict[str, Any]]:
    if not raw:
        return {}
    out: dict[int, dict[str, Any]] = {}
    for key, value in raw.items():
        try:
            out[int(key)] = value
        except (TypeError, ValueError):
            continue
    return out
