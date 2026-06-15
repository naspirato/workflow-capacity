"""Shard count selection for PR-check parallel path."""

from __future__ import annotations

DEFAULT_THREADS = 52
DEFAULT_LIGHT_THRESHOLD_MIN = 60.0
DEFAULT_TIERS = ((120.0, 4), (200.0, 8), (float("inf"), 12))
DEFAULT_PEAK_HOURS_UTC = range(8, 19)
DEFAULT_PEAK_CAP = 4


def estimate_single_job_minutes(total_weight_sec: float, threads: int) -> float:
    if threads <= 0:
        raise ValueError("threads must be positive")
    return total_weight_sec / 60.0 / threads


def choose_shard_count(
    total_weight_sec: float,
    *,
    threads: int = DEFAULT_THREADS,
    light_threshold_min: float = DEFAULT_LIGHT_THRESHOLD_MIN,
    peak_cap: int = DEFAULT_PEAK_CAP,
    is_peak: bool = False,
    max_shards: int = 0,
) -> tuple[int, float]:
    estimate_min = estimate_single_job_minutes(total_weight_sec, threads)
    if estimate_min < light_threshold_min:
        count = 1
    else:
        count = DEFAULT_TIERS[-1][1]
        for upper_min, tier_count in DEFAULT_TIERS:
            if estimate_min < upper_min:
                count = tier_count
                break
    if is_peak and count > peak_cap:
        count = peak_cap
    if max_shards > 0:
        count = min(count, max_shards)
    return max(count, 1), estimate_min


def is_peak_hour_utc(hour: int) -> bool:
    return hour in DEFAULT_PEAK_HOURS_UTC
