"""Discrete-event runner pool simulator."""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field

from workflow_capacity.config import PoolConfig, RESOURCES


@dataclass
class ActiveJob:
    end_time: float
    preset: str
    job_key: str


@dataclass
class PoolSimulator:
    config: PoolConfig
    active: list[ActiveJob] = field(default_factory=list)
    queue_wait_sec: float = 0.0
    queued_events: int = 0
    peak_instances: int = 0
    instance_seconds: float = 0.0
    saturated_seconds: float = 0.0
    last_time: float = 0.0

    def _active_by_label(self) -> Counter:
        counts: Counter = Counter()
        for job in self.active:
            counts[job.preset] += 1
        return counts

    def _expire(self, now: float) -> None:
        if now <= self.last_time:
            return
        alive = [job for job in self.active if job.end_time > self.last_time]
        dt = now - self.last_time
        self.instance_seconds += len(alive) * dt
        budget = self.config.max_instances_budget()
        if budget > 0 and len(alive) >= budget * 0.9:
            self.saturated_seconds += dt
        self.active = [job for job in self.active if job.end_time > now]
        self.last_time = now

    def max_new_runners(self, preset_label: str) -> int:
        used = {res: 0.0 for res in RESOURCES}
        used_instances = 0
        for label, count in self._active_by_label().items():
            fp = self.config.footprint(label)
            for res in RESOURCES:
                used[res] += fp.__dict__[res] * count
            used_instances += count

        budget = self.config.available_budget()
        free_instances = budget["instances"] - used_instances
        fp = self.config.footprint(preset_label)
        fits = [free_instances]
        for res in RESOURCES:
            fits.append((budget[res] - used[res]) / max(fp.__dict__[res], 1))
        return max(int(math.floor(min(fits))), 0)

    def can_allocate(self, preset: str, count: int = 1) -> bool:
        return self.max_new_runners(preset) >= count

    def capacity_cap(self, preset: str) -> int:
        max_new = self.max_new_runners(preset)
        floor = self.config.saturated_min_shards
        return max(max_new, floor)

    def allocate(self, now: float, duration_sec: float, preset: str, job_key: str) -> float:
        self._expire(now)
        wait = 0.0
        while not self.can_allocate(preset):
            if not self.active:
                break
            next_free = min(job.end_time for job in self.active)
            wait += next_free - now
            self._expire(next_free)
            now = next_free
            self.queued_events += 1
        end = now + duration_sec
        self.active.append(ActiveJob(end_time=end, preset=preset, job_key=job_key))
        self.queue_wait_sec += wait
        self.peak_instances = max(self.peak_instances, len(self.active))
        self.last_time = now
        return wait

    def allocate_parallel(
        self, now: float, duration_sec: float, preset: str, count: int, job_key: str
    ) -> float:
        self._expire(now)
        wait = 0.0
        while self.max_new_runners(preset) < count:
            if not self.active:
                break
            next_free = min(job.end_time for job in self.active)
            wait += next_free - now
            self._expire(next_free)
            now = next_free
            self.queued_events += 1
        for idx in range(count):
            end = now + duration_sec
            self.active.append(
                ActiveJob(end_time=end, preset=preset, job_key=f"{job_key}:{idx}")
            )
        self.queue_wait_sec += wait
        self.peak_instances = max(self.peak_instances, len(self.active))
        self.last_time = now
        return wait

    def finalize(self, end_time: float) -> None:
        self._expire(end_time)
