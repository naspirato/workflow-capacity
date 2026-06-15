"""Bottleneck analysis and reverse quota calculator for concurrent runners."""

from __future__ import annotations

import math
from typing import Any

from workflow_capacity.config import PoolConfig, RESOURCES

RWDI_PRESET = "build-preset-relwithdebinfo"
ASAN_PRESET = "build-preset-release-asan"

BINDING_LABELS = {
    "instances": "instances",
    "vcpu": "vCPU",
    "ram_gb": "RAM",
    "nrd_ssd_gb": "SSD",
}

QUOTA_KEYS = ("instances", "vcpu", "ram_gb", "nrd_ssd_gb")


def _runner_step(cfg: PoolConfig, preset: str) -> dict[str, int]:
    fp = cfg.footprint(preset)
    return {
        "instances": 1,
        "vcpu": fp.vcpu,
        "ram_gb": fp.ram_gb,
        "nrd_ssd_gb": fp.nrd_ssd_gb,
    }


def pr_check_pair_step(cfg: PoolConfig) -> dict[str, int]:
    """One PR-check run holds relwithdebinfo + release-asan at the same time."""
    rwdi = _runner_step(cfg, RWDI_PRESET)
    asan = _runner_step(cfg, ASAN_PRESET)
    return {
        "instances": rwdi["instances"] + asan["instances"],
        "vcpu": rwdi["vcpu"] + asan["vcpu"],
        "ram_gb": rwdi["ram_gb"] + asan["ram_gb"],
        "nrd_ssd_gb": rwdi["nrd_ssd_gb"] + asan["nrd_ssd_gb"],
    }


def max_slots_for_step(cfg: PoolConfig, step: dict[str, int]) -> tuple[str, dict[str, int], int]:
    budget = cfg.available_budget()
    slots: dict[str, int] = {}
    for res in ("instances", *RESOURCES):
        per = max(step[res], 1)
        slots[res] = int(budget[res] / per)
    binding = min(slots, key=slots.get)
    return binding, slots, int(slots[binding])


def _max_rwdi_slots(cfg: PoolConfig, preset: str) -> tuple[str, dict[str, int], int]:
    return max_slots_for_step(cfg, _runner_step(cfg, preset))


def _min_quota_for_slots(
    cfg: PoolConfig,
    step: dict[str, int],
    resource: str,
    target_slots: int,
) -> int:
    """Smallest folder quota for resource that still yields >= target_slots."""
    static = cfg.static_usage()[resource]
    hf = cfg.headroom_fraction
    per = max(step[resource], 1)
    raw = static + (target_slots * per) / hf
    return max(int(math.ceil(raw)), int(static) + per)


def _quota_trim_for_resource(
    cfg: PoolConfig,
    step: dict[str, int],
    resource: str,
    *,
    cap: int,
    current_slots: int,
) -> dict[str, int]:
    current = int(cfg.quotas[resource])
    if current_slots <= cap:
        return {"quota_current": current, "quota_trim": 0, "quota_min": current}
    quota_min = _min_quota_for_slots(cfg, step, resource, cap)
    quota_trim = max(0, current - quota_min)
    return {
        "quota_current": current,
        "quota_trim": quota_trim,
        "quota_min": current - quota_trim,
    }


def config_for_target_step(
    cfg: PoolConfig,
    step: dict[str, int],
    target_slots: int,
    *,
    name: str | None = None,
) -> PoolConfig:
    """Adjust quotas (binding resource) until max concurrent units >= target."""
    target = max(1, int(target_slots))
    quotas = {k: int(v) for k, v in cfg.quotas.items()}
    trial = cfg
    label = name or f"{cfg.name}_n{target}"

    for _ in range(20_000):
        _, _, cur = max_slots_for_step(trial, step)
        if cur == target:
            return PoolConfig.from_dict({**trial.to_dict(), "quotas": quotas}, name=label)
        binding, _, _ = max_slots_for_step(trial, step)
        if cur < target:
            quotas[binding] = int(quotas[binding]) + step[binding]
        else:
            quotas[binding] = max(step[binding], int(quotas[binding]) - step[binding])
        trial = PoolConfig.from_dict({**cfg.to_dict(), "quotas": quotas}, name=label)

    return trial


def config_for_target_slots(
    cfg: PoolConfig,
    preset: str,
    target_slots: int,
    *,
    name: str | None = None,
) -> PoolConfig:
    return config_for_target_step(
        cfg, _runner_step(cfg, preset), target_slots, name=name
    )


def analyze_for_step(
    cfg: PoolConfig,
    step: dict[str, int],
    *,
    kind: str,
    label: str,
    presets: list[str] | None = None,
) -> dict[str, Any]:
    binding, slots, cap = max_slots_for_step(cfg, step)
    resources: list[dict[str, Any]] = []
    for res in ("instances", *RESOURCES):
        count = int(slots[res])
        slack = count - cap
        trim = _quota_trim_for_resource(
            cfg, step, res, cap=cap, current_slots=count
        )
        resources.append(
            {
                "resource": res,
                "label": BINDING_LABELS[res],
                "slots": count,
                "binding": res == binding,
                "slack_runners": slack,
                **trim,
            }
        )

    plus_one_cfg = config_for_target_step(cfg, step, cap + 1, name=f"{cfg.name}_plus1")
    _, _, plus_one_cap = max_slots_for_step(plus_one_cfg, step)
    base_q = {k: int(cfg.quotas[k]) for k in QUOTA_KEYS}
    plus_one_quotas = {k: int(v) for k, v in plus_one_cfg.quotas.items()}
    plus_one_delta = {k: plus_one_quotas[k] - base_q[k] for k in QUOTA_KEYS}

    out: dict[str, Any] = {
        "kind": kind,
        "label": label,
        "binding": binding,
        "binding_label": BINDING_LABELS[binding],
        "max_concurrent": cap,
        "quotas": base_q,
        "runner_footprint": step,
        "slots": {k: int(v) for k, v in slots.items()},
        "resources": resources,
        "plus_one_runner": {
            "delta_quotas": {k: v for k, v in plus_one_delta.items() if v},
            "new_quotas": plus_one_quotas,
            "quota_delta": plus_one_delta,
            "new_max_concurrent": plus_one_cap,
        },
    }
    if presets:
        out["presets"] = presets
    return out


def analyze_preset(cfg: PoolConfig, preset: str) -> dict[str, Any]:
    step = _runner_step(cfg, preset)
    return analyze_for_step(
        cfg,
        step,
        kind="preset",
        label=preset,
        presets=[preset],
    )


def analyze_pr_check(cfg: PoolConfig) -> dict[str, Any]:
    step = pr_check_pair_step(cfg)
    rwdi = _runner_step(cfg, RWDI_PRESET)
    asan = _runner_step(cfg, ASAN_PRESET)
    return analyze_for_step(
        cfg,
        step,
        kind="pr_check_pair",
        label="PR-check (relwithdebinfo + release-asan)",
        presets=[RWDI_PRESET, ASAN_PRESET],
    ) | {
        "branch_footprints": {"rwdi": rwdi, "asan": asan},
    }


def analyze_bottleneck(cfg: PoolConfig) -> dict[str, Any]:
    pr_check = analyze_pr_check(cfg)
    rwdi = analyze_preset(cfg, RWDI_PRESET)
    asan = analyze_preset(cfg, ASAN_PRESET)
    trimmable = [
        {
            "resource": r["resource"],
            "label": r["label"],
            "quota_current": r["quota_current"],
            "quota_trim": r["quota_trim"],
            "quota_min": r["quota_min"],
        }
        for r in pr_check["resources"]
        if not r["binding"] and r["quota_trim"] > 0
    ]
    return {
        "quotas": pr_check["quotas"],
        "pr_check": pr_check,
        "rwdi": rwdi,
        "asan": asan,
        "non_binding_headroom_rwdi": [
            r for r in rwdi["resources"] if not r["binding"] and r["slack_runners"] > 0
        ],
        "quota_trim_rwdi": trimmable,
        "quota_trim": trimmable,
    }


def runner_plan_steps(
    cfg: PoolConfig,
    *,
    delta_min: int = -3,
    delta_max: int = 10,
) -> dict[str, Any]:
    step = pr_check_pair_step(cfg)
    _, _, baseline = max_slots_for_step(cfg, step)
    steps: list[dict[str, Any]] = []
    seen_targets: set[int] = set()
    for delta in range(delta_min, delta_max + 1):
        target = max(1, baseline + delta)
        if target in seen_targets:
            continue
        seen_targets.add(target)
        new_cfg = config_for_target_step(
            cfg, step, target, name=f"{cfg.name}_d{delta:+d}"
        )
        _, slots, cap = max_slots_for_step(new_cfg, step)
        binding, _, _ = max_slots_for_step(new_cfg, step)
        quotas = {k: int(v) for k, v in new_cfg.quotas.items()}
        base_q = {k: int(v) for k, v in cfg.quotas.items()}
        steps.append(
            {
                "delta_runners": delta,
                "delta_pairs": delta,
                "target_pairs": cap,
                "target_rwdi": cap,
                "binding": binding,
                "binding_label": BINDING_LABELS[binding],
                "slots": {k: int(v) for k, v in slots.items()},
                "quotas": quotas,
                "quota_delta": {k: quotas[k] - base_q[k] for k in QUOTA_KEYS},
            }
        )
    steps.sort(key=lambda s: s["delta_runners"])
    return {
        "baseline_pairs": baseline,
        "baseline_rwdi": baseline,
        "steps": steps,
    }
