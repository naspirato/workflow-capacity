"""Notebook-friendly progress lines (stdout, flushed immediately)."""

from __future__ import annotations


def status(msg: str) -> None:
    print(msg, flush=True)
