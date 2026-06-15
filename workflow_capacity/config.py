"""Load and override runner pool capacity configuration."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

RESOURCES = ("vcpu", "ram_gb", "nrd_ssd_gb")
DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "config" / "capacity.example.yml"


@dataclass
class RunnerFootprint:
    vcpu: int
    ram_gb: int
    nrd_ssd_gb: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunnerFootprint:
        return cls(
            vcpu=int(data["vcpu"]),
            ram_gb=int(data["ram_gb"]),
            nrd_ssd_gb=int(data["nrd_ssd_gb"]),
        )

    def scaled(self, mult: float) -> RunnerFootprint:
        return RunnerFootprint(
            vcpu=max(1, int(self.vcpu * mult)),
            ram_gb=max(1, int(self.ram_gb * mult)),
            nrd_ssd_gb=max(1, int(self.nrd_ssd_gb * mult)),
        )


@dataclass
class StaticRunner:
    """Always-on runners (tiny workers, cache nodes, etc.)."""

    label: str
    count: int
    footprint: RunnerFootprint


@dataclass
class PoolConfig:
    quotas: dict[str, float]
    reserved: dict[str, float]
    headroom_fraction: float
    footprints: dict[str, RunnerFootprint]
    default_footprint: RunnerFootprint
    static_runners: list[StaticRunner] = field(default_factory=list)
    saturated_min_shards: int = 1
    name: str = "default"

    @classmethod
    def load(cls, path: Path | str, *, name: str | None = None) -> PoolConfig:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(raw, name=name or Path(path).stem)

    @classmethod
    def from_dict(cls, raw: dict[str, Any], *, name: str = "custom") -> PoolConfig:
        static: list[StaticRunner] = []
        for label, spec in (raw.get("static_runners") or {}).items():
            static.append(
                StaticRunner(
                    label=label,
                    count=int(spec.get("count", 0)),
                    footprint=RunnerFootprint.from_dict(spec["footprint"]),
                )
            )
        footprints = {
            label: RunnerFootprint.from_dict(fp)
            for label, fp in (raw.get("footprints") or {}).items()
        }
        return cls(
            quotas={k: float(v) for k, v in raw["quotas"].items()},
            reserved={k: float(v) for k, v in (raw.get("reserved") or {}).items()},
            headroom_fraction=float(raw.get("headroom_fraction", 1.0)),
            footprints=footprints,
            default_footprint=RunnerFootprint.from_dict(raw["default_footprint"]),
            static_runners=static,
            saturated_min_shards=int(raw.get("saturated_min_shards", 1)),
            name=name,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "quotas": {k: int(v) for k, v in self.quotas.items()},
            "reserved": {k: int(v) for k, v in self.reserved.items()},
            "headroom_fraction": self.headroom_fraction,
            "static_runners": {
                s.label: {"count": s.count, "footprint": s.footprint.__dict__}
                for s in self.static_runners
            },
            "footprints": {k: v.__dict__ for k, v in self.footprints.items()},
            "default_footprint": self.default_footprint.__dict__,
            "saturated_min_shards": self.saturated_min_shards,
        }

    def footprint(self, preset_label: str) -> RunnerFootprint:
        return self.footprints.get(preset_label, self.default_footprint)

    def static_usage(self) -> dict[str, float]:
        used = {res: 0.0 for res in RESOURCES}
        used["instances"] = 0.0
        for runner in self.static_runners:
            fp = runner.footprint
            used["instances"] += runner.count
            for res in RESOURCES:
                used[res] += fp.__dict__[res] * runner.count
        for res in RESOURCES:
            used[res] += self.reserved.get(res, 0.0)
        used["instances"] += self.reserved.get("instances", 0.0)
        return used

    def available_budget(self) -> dict[str, float]:
        static = self.static_usage()
        return {
            res: (self.quotas.get(res, 0.0) - static.get(res, 0.0)) * self.headroom_fraction
            for res in ("instances", *RESOURCES)
        }

    def max_instances_budget(self) -> float:
        return self.available_budget()["instances"]

    def concurrent_slots(self, preset_label: str) -> dict[str, int]:
        """Max concurrent runners of preset_label if only one resource bound."""
        budget = self.available_budget()
        fp = self.footprint(preset_label)
        return {
            "instances": int(budget["instances"]),
            "vcpu": int(budget["vcpu"] / max(fp.vcpu, 1)),
            "ram_gb": int(budget["ram_gb"] / max(fp.ram_gb, 1)),
            "nrd_ssd_gb": int(budget["nrd_ssd_gb"] / max(fp.nrd_ssd_gb, 1)),
        }

    def binding_for_preset(self, preset_label: str) -> tuple[str, dict[str, int]]:
        slots = self.concurrent_slots(preset_label)
        binding = min(slots, key=slots.get)
        return binding, slots

    def with_quota_scale(
        self,
        *,
        instances: float = 1.0,
        vcpu: float = 1.0,
        ram_gb: float = 1.0,
        nrd_ssd_gb: float = 1.0,
        name: str | None = None,
    ) -> PoolConfig:
        raw = self.to_dict()
        raw["quotas"]["instances"] = int(raw["quotas"]["instances"] * instances)
        raw["quotas"]["vcpu"] = int(raw["quotas"]["vcpu"] * vcpu)
        raw["quotas"]["ram_gb"] = int(raw["quotas"]["ram_gb"] * ram_gb)
        raw["quotas"]["nrd_ssd_gb"] = int(raw["quotas"]["nrd_ssd_gb"] * nrd_ssd_gb)
        return PoolConfig.from_dict(raw, name=name or f"{self.name}_scaled")

    def with_overrides(self, overrides: dict[str, Any], *, name: str | None = None) -> PoolConfig:
        raw = self.to_dict()
        for key, value in overrides.items():
            if key == "quotas" and isinstance(value, dict):
                raw["quotas"].update({k: int(v) for k, v in value.items()})
            elif key == "footprints" and isinstance(value, dict):
                raw["footprints"].update(value)
            elif key == "static_runners" and isinstance(value, dict):
                raw["static_runners"].update(value)
            else:
                raw[key] = value
        return PoolConfig.from_dict(raw, name=name or self.name)
