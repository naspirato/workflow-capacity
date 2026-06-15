"""Export simulation JSON for capacity_comparison.html."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workflow_capacity.bottleneck import (
    ASAN_PRESET,
    BINDING_LABELS,
    RWDI_PRESET,
    max_slots_for_step,
    pr_check_pair_step,
)
from workflow_capacity.cache import JobsDataset
from workflow_capacity.compare import evaluate_config
from workflow_capacity.config import PoolConfig
from workflow_capacity.log import status
from workflow_capacity.metrics import (
    D_GROUP_LABELS,
    D_GROUPS,
    ROLL_OUTS,
    agg_get,
    comparison_table,
    normalize_percentiles,
    pct_label,
)
from workflow_capacity.pr_classify import ClassifyRules
from workflow_capacity.simulate import PR_LOAD_STAGGER_SEC

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "config" / "capacity.example.yml"
DEFAULT_PEAK_HOURS = list(range(8, 19))
CHART_HOURS = list(range(24))

NAMED_SCENARIOS = (
    ("current", {}),
    ("all-20%", {"instances": 0.8, "vcpu": 0.8, "ram_gb": 0.8, "nrd_ssd_gb": 0.8}),
    ("all-10%", {"instances": 0.9, "vcpu": 0.9, "ram_gb": 0.9, "nrd_ssd_gb": 0.9}),
    ("all+10%", {"instances": 1.1, "vcpu": 1.1, "ram_gb": 1.1, "nrd_ssd_gb": 1.1}),
    ("all+25%", {"instances": 1.25, "vcpu": 1.25, "ram_gb": 1.25, "nrd_ssd_gb": 1.25}),
    ("all x2", {"instances": 2.0, "vcpu": 2.0, "ram_gb": 2.0, "nrd_ssd_gb": 2.0}),
)

# Joint scaling of all quota dimensions (excluding 1.0 — covered by "current").
SCALE_SWEEPS = (0.80, 0.85, 0.90, 0.95, 1.05, 1.10, 1.15, 1.25, 1.50, 2.0)

# PR-check arrival rate: 90% … 130% in 5% steps (weekday runs, deterministic).
LOAD_SWEEPS = tuple(round(i * 0.05, 2) for i in range(18, 27))

RUNNER_DELTA_MIN = -3
RUNNER_DELTA_MAX = 10

PR_WALL_NOTE = (
    "PR-check = max(relwithdebinfo, release-asan); в шардинге оба job через prepare + shards"
)


def _round_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: round(v, 2) if isinstance(v, float) else v for k, v in row.items()}


def _metrics_pair(
    base_agg: dict,
    par_agg: dict,
    key: tuple,
    *,
    percentiles: list[float],
    primary_percentile: float,
) -> dict[str, Any] | None:
    b = base_agg.get(key)
    p = par_agg.get(key)
    if not b or not p:
        return None
    row: dict[str, Any] = {"n": max(b.get("n", 0), p.get("n", 0))}
    for pct in percentiles:
        pl = pct_label(pct)
        bw = agg_get(b, "wait", pct)
        bb = agg_get(b, "work", pct)
        bt = agg_get(b, "total", pct)
        pw = agg_get(p, "wait", pct)
        pb = agg_get(p, "work", pct)
        pt = agg_get(p, "total", pct)
        if bt is None or pt is None:
            return None
        row[f"mono_wait_{pl}"] = bw
        row[f"mono_work_{pl}"] = bb
        row[f"mono_total_{pl}"] = bt
        row[f"shard_wait_{pl}"] = pw
        row[f"shard_work_{pl}"] = pb
        row[f"shard_total_{pl}"] = pt
        row[f"delta_wait_{pl}"] = (pw or 0) - (bw or 0)
        row[f"delta_work_{pl}"] = (pb or 0) - (bb or 0)
        row[f"delta_total_{pl}"] = pt - bt
        row[f"delta_total_pct_{pl}"] = 100.0 * (pt - bt) / bt if bt else None
    pl = pct_label(primary_percentile)
    row["delta_wait"] = row.get(f"delta_wait_{pl}")
    row["delta_work"] = row.get(f"delta_work_{pl}")
    row["delta_total"] = row.get(f"delta_total_{pl}")
    row["delta_total_pct"] = row.get(f"delta_total_pct_{pl}")
    return _round_row(row)


def _quota_key(quotas: dict[str, int]) -> tuple[int, ...]:
    return (
        int(quotas["instances"]),
        int(quotas["vcpu"]),
        int(quotas["ram_gb"]),
        int(quotas["nrd_ssd_gb"]),
    )


def _scale_label(scale: float) -> str:
    pct = int(round(scale * 100))
    return f"scale={pct}%"


def _load_label(load: float) -> str:
    pct = int(round(load * 100))
    return f"load={pct}%"


def _grid_name(quota_scale: float, load: float) -> str:
    if abs(quota_scale - 1.0) < 1e-6 and abs(load - 1.0) < 1e-6:
        return "current"
    parts: list[str] = []
    if abs(quota_scale - 1.0) > 1e-6:
        parts.append(f"quotas={int(round(quota_scale * 100))}%")
    if abs(load - 1.0) > 1e-6:
        parts.append(f"load={int(round(load * 100))}%")
    return " ".join(parts)


def _pr_count_by_hour(baseline_rows: list, parallel_rows: list) -> list[dict[str, int | str]]:
    from collections import Counter

    mono = Counter(r.hour_utc for r in baseline_rows)
    shard = Counter(r.hour_utc for r in parallel_rows)
    return [
        {
            "hour_utc": f"{hour:02d}:00",
            "mono_n": mono.get(hour, 0),
            "shard_n": shard.get(hour, 0),
        }
        for hour in CHART_HOURS
    ]


def _capacity_summary(
    cfg: PoolConfig,
    *,
    baseline,
    parallel,
) -> dict[str, Any]:
    step = pr_check_pair_step(cfg)
    binding, slots, max_pr = max_slots_for_step(cfg, step)
    budget = cfg.max_instances_budget()
    peak_mono = baseline.pool.peak_instances
    peak_shard = parallel.pool.peak_instances
    return {
        "binding": binding,
        "binding_label": BINDING_LABELS[binding],
        "max_pr_check": max_pr,
        "max_rwdi": max_pr,
        "slots": slots,
        "vm_budget": round(budget, 1),
        "peak_mono": peak_mono,
        "peak_shard": peak_shard,
        "queued_mono": baseline.pool.queued_events,
        "queued_shard": parallel.pool.queued_events,
        "instances_saturated_mono": peak_mono >= budget * 0.98,
        "instances_saturated_shard": peak_shard >= budget * 0.98,
    }


def _evaluate_point(
    jobs: list[dict[str, Any]],
    pr_runs: list,
    cfg: PoolConfig,
    *,
    rollout_label: str,
    shard_eligible,
    peak_hours: list[int],
    percentiles: list[float],
    primary_percentile: float,
    load_scale: float = 1.0,
) -> dict[str, Any]:
    from workflow_capacity.simulate import scale_pr_traffic

    sim_jobs, sim_runs = scale_pr_traffic(jobs, pr_runs, load_scale)
    item = evaluate_config(
        sim_jobs,
        sim_runs,
        cfg,
        rollout_label=rollout_label,
        shard_eligible=shard_eligible,
        peak_hours=peak_hours,
        pr_wall=True,
        percentiles=percentiles,
        primary_percentile=primary_percentile,
        load_scale=1.0,
    )
    overall = _metrics_pair(
        item.base_agg,
        item.par_agg,
        (None, "all"),
        percentiles=percentiles,
        primary_percentile=primary_percentile,
    )
    return {
        "quotas": {k: int(v) for k, v in cfg.quotas.items()},
        "vm_budget": round(cfg.max_instances_budget(), 1),
        "capacity": _capacity_summary(cfg, baseline=item.baseline, parallel=item.parallel),
        "pr_runs_simulated": len(sim_runs),
        "overall": overall,
        "by_d_group": [
            _round_row({**row, "d_group": d_label})
            for d_key, d_label in D_GROUP_LABELS.items()
            if d_key != "all"
            for row in [
                _metrics_pair(
                    item.base_agg,
                    item.par_agg,
                    (None, d_key),
                    percentiles=percentiles,
                    primary_percentile=primary_percentile,
                )
            ]
            if row
        ],
        "by_hour": [
            _round_row(r)
            for r in comparison_table(
                item.base_agg,
                item.par_agg,
                hours=CHART_HOURS,
                d_keys=["all"],
                percentiles=percentiles,
                primary_percentile=primary_percentile,
            )
        ],
        "by_hour_d_group": [
            _round_row(r)
            for r in comparison_table(
                item.base_agg,
                item.par_agg,
                hours=CHART_HOURS,
                d_keys=[k for k, _ in D_GROUPS if k != "all"],
                percentiles=percentiles,
                primary_percentile=primary_percentile,
            )
        ],
        "pr_count_by_hour": _pr_count_by_hour(item.baseline_rows, item.parallel_rows),
    }


def build_all_configs(base: PoolConfig) -> list[tuple[str, PoolConfig, float]]:
    out: list[tuple[str, PoolConfig, float]] = []
    seen: set[tuple[int, ...]] = set()

    for name, scale in NAMED_SCENARIOS:
        mult = 1.0
        if scale:
            cfg = base.with_quota_scale(name=name, **scale)
            if len(set(scale.values())) == 1:
                mult = next(iter(scale.values()))
        else:
            cfg = base
        key = _quota_key(cfg.quotas)
        if key not in seen:
            seen.add(key)
            out.append((name, cfg, mult))

    for mult in SCALE_SWEEPS:
        name = _scale_label(mult)
        cfg = base.with_quota_scale(
            name=name,
            instances=mult,
            vcpu=mult,
            ram_gb=mult,
            nrd_ssd_gb=mult,
        )
        key = _quota_key(cfg.quotas)
        if key in seen:
            continue
        seen.add(key)
        out.append((name, cfg, mult))
    return out


def _export_runner_plans(
    jobs: list[dict[str, Any]],
    pr_runs: list,
    configs: list[tuple[str, PoolConfig, float]],
    *,
    rollout_label: str,
    shard_eligible,
    peak_hours: list[int],
    percentiles: list[float],
    primary_percentile: float,
) -> dict[str, Any]:
    from workflow_capacity.bottleneck import analyze_bottleneck, runner_plan_steps

    profiles: list[tuple[PoolConfig, float]] = []
    seen_keys: set[tuple[int, ...]] = set()
    for _name, cfg, quota_scale in configs:
        key = _quota_key(cfg.quotas)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        profiles.append((cfg, quota_scale))

    profile_plans = [
        (
            cfg,
            quota_scale,
            runner_plan_steps(
                cfg,
                delta_min=RUNNER_DELTA_MIN,
                delta_max=RUNNER_DELTA_MAX,
            ),
        )
        for cfg, quota_scale in profiles
    ]
    total_steps = sum(len(plan["steps"]) for _, _, plan in profile_plans)
    status(f"export: runner calculator ({total_steps} simulations) ...")

    plans: dict[str, Any] = {}
    step_idx = 0
    for cfg, quota_scale, plan in profile_plans:
        scale_key = str(round(quota_scale, 4))
        scale_label = f"quotas={int(round(quota_scale * 100))}%"
        bottleneck = analyze_bottleneck(cfg)
        steps_out: list[dict[str, Any]] = []
        for step in plan["steps"]:
            step_idx += 1
            delta = step["delta_runners"]
            status(
                f"[runner {step_idx}/{total_steps}] {scale_label} "
                f"Δ={delta:+d} → {step.get('target_pairs', step.get('target_rwdi', '?'))} PR-check ..."
            )
            step_cfg = PoolConfig.from_dict(
                {**cfg.to_dict(), "quotas": step["quotas"]},
                name=f"{cfg.name}_run{step['delta_runners']:+d}",
            )
            point = _evaluate_point(
                jobs,
                pr_runs,
                step_cfg,
                rollout_label=rollout_label,
                shard_eligible=shard_eligible,
                peak_hours=peak_hours,
                percentiles=percentiles,
                primary_percentile=primary_percentile,
                load_scale=1.0,
            )
            steps_out.append(
                {
                    **step,
                    "vm_budget": point["vm_budget"],
                    "capacity": point["capacity"],
                    "overall": point["overall"],
                    "pr_runs_simulated": point["pr_runs_simulated"],
                }
            )
        plans[scale_key] = {
            "scale": round(quota_scale, 4),
            "baseline_pairs": plan["baseline_pairs"],
            "baseline_rwdi": plan["baseline_rwdi"],
            "bottleneck": bottleneck,
            "steps": steps_out,
        }
    status(f"runner plans: {len(plans)} quota profiles, {total_steps} simulated steps")
    return plans


def export_results(
    jobs: list[dict[str, Any]],
    *,
    config_path: Path = DEFAULT_CONFIG,
    classify: bool = True,
    rollout_label: str = "all eligible",
    peak_hours: list[int] | None = None,
    pr_files: dict[str, Any] | None = None,
    classify_rules: ClassifyRules | None = None,
    percentiles: float | int | list[float] | None = None,
    primary_percentile: float | None = None,
) -> dict[str, Any]:
    from workflow_capacity.pr_check import build_pr_check_runs

    peak_hours = peak_hours if peak_hours is not None else DEFAULT_PEAK_HOURS
    pcts = normalize_percentiles(percentiles)
    primary = float(primary_percentile if primary_percentile is not None else pcts[-1])
    rules = classify_rules or ClassifyRules.load(config_path)
    base = PoolConfig.load(config_path, name="current")
    rollout = next(r for r in ROLL_OUTS if r[1] == rollout_label)
    _, _, shard_eligible = rollout

    effective_classify = classify
    classify_note: str | None = None
    if classify and not pr_files:
        effective_classify = False
        classify_note = (
            "pr_files missing in cache — classify disabled, all PR-check runs treated as sharded"
        )
        status(f"export: {classify_note}")

    pr_runs = build_pr_check_runs(
        jobs,
        classify=effective_classify,
        pr_files=pr_files,
        classify_rules=rules,
    )

    from workflow_capacity.simulate import run_pair

    mono_run, shard_run = run_pair(
        jobs, pr_runs, base, shard_eligible=shard_eligible
    )
    static = base.static_usage()
    current_capacity = _capacity_summary(base, baseline=mono_run, parallel=shard_run)
    pool_meta = {
        "instances_quota": int(base.quotas["instances"]),
        "static_instances": int(static["instances"]),
        "static_vcpu": int(static["vcpu"]),
        "runner_budget_instances": current_capacity["vm_budget"],
        "peak_instances_mono": current_capacity["peak_mono"],
        "peak_instances_shard": current_capacity["peak_shard"],
        "queued_events_mono": current_capacity["queued_mono"],
        "queued_events_shard": current_capacity["queued_shard"],
        "binding": current_capacity["binding"],
        "binding_label": current_capacity["binding_label"],
        "max_rwdi": current_capacity["max_rwdi"],
        "slots": current_capacity["slots"],
    }

    scenarios: list[dict[str, Any]] = []
    interactive: list[dict[str, Any]] = []
    configs = build_all_configs(base)
    total = len(configs) * len(LOAD_SWEEPS)
    idx = 0

    for name, cfg, quota_scale in configs:
        for load in LOAD_SWEEPS:
            idx += 1
            grid_name = _grid_name(quota_scale, load)
            status(f"[export {idx}/{total}] {grid_name} ...")
            point = _evaluate_point(
                jobs,
                pr_runs,
                cfg,
                rollout_label=rollout_label,
                shard_eligible=shard_eligible,
                peak_hours=peak_hours,
                percentiles=pcts,
                primary_percentile=primary,
                load_scale=load,
            )
            entry = {
                "name": grid_name,
                "scale": round(quota_scale, 4),
                "load": round(load, 4),
                **point,
            }
            interactive.append(entry)
            if name in {n for n, _ in NAMED_SCENARIOS} and abs(load - 1.0) < 1e-6:
                scenarios.append(entry)

    interactive.sort(key=lambda item: (item["scale"], item["load"]))
    scale_sweeps = sorted({item["scale"] for item in interactive})
    load_sweeps = sorted({item["load"] for item in interactive})
    base_q = {k: int(v) for k, v in base.quotas.items()}
    runner_plans = _export_runner_plans(
        jobs,
        pr_runs,
        configs,
        rollout_label=rollout_label,
        shard_eligible=shard_eligible,
        peak_hours=peak_hours,
        percentiles=pcts,
        primary_percentile=primary,
    )
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "jobs_count": len(jobs),
            "pr_runs_count": len(pr_runs),
            "classify": effective_classify,
            "classify_requested": classify,
            "classify_from_cache": bool(pr_files),
            "classify_note": classify_note,
            "rollout": rollout_label,
            "peak_hours": peak_hours,
            "chart_hours": CHART_HOURS,
            "percentiles": pcts,
            "primary_percentile": primary,
            "pr_wall": True,
            "note": PR_WALL_NOTE,
            "pool": pool_meta,
            "binding_model": "pr_check_pair",
            "binding_presets": [RWDI_PRESET, ASAN_PRESET],
            "load_note": (
                "90–130% потока PR-check (шаг 5%): load<100% — детерминированная выборка run'ов; "
                f"load>100% — доп. копии (первая одновременно с оригиналом, далее +{int(PR_LOAD_STAGGER_SEC)}с). "
                "Остальные workflow без изменений."
            ),
        },
        "base_quotas": base_q,
        "scale_sweeps": scale_sweeps,
        "load_sweeps": load_sweeps,
        "scenarios": scenarios,
        "interactive": interactive,
        "runner_plans": runner_plans,
        "runner_delta_min": RUNNER_DELTA_MIN,
        "runner_delta_max": RUNNER_DELTA_MAX,
    }


def export_from_dataset(
    dataset: JobsDataset,
    *,
    config_path: Path = DEFAULT_CONFIG,
    classify: bool = True,
    rollout_label: str = "all eligible",
    peak_hours: list[int] | None = None,
    percentiles: float | int | list[float] | None = None,
    primary_percentile: float | None = None,
) -> dict[str, Any]:
    """Build comparison payload from a cached JobsDataset."""
    payload = export_results(
        dataset.jobs,
        config_path=config_path,
        classify=classify,
        rollout_label=rollout_label,
        peak_hours=peak_hours,
        pr_files=dataset.pr_files,
        percentiles=percentiles,
        primary_percentile=primary_percentile,
    )
    payload["meta"]["repo"] = dataset.repo
    payload["meta"]["since"] = dataset.since
    payload["meta"]["until"] = dataset.until
    payload["meta"]["dataset"] = dataset.path.name
    payload["meta"]["dataset_partial"] = dataset.path.name.endswith(".partial.json")
    payload["meta"]["collect_stats"] = dataset.stats or None
    payload["meta"]["weekday_only"] = True
    payload["meta"]["hourly_note"] = (
        "Почасовые графики: p90 по всем PR-check run'ам в bucket HH:00 UTC "
        "за весь интервал (будни); не один календарный день."
    )
    return payload


_SIMULATION_DATA_RE = re.compile(
    r'(<script id="simulation-data" type="application/json">)(.*?)(</script>)',
    re.DOTALL,
)


def _embed_payload_in_html(html: str, payload: dict[str, Any]) -> str:
    json_text = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    replacement = rf'\1{json_text}\3'
    if _SIMULATION_DATA_RE.search(html):
        return _SIMULATION_DATA_RE.sub(replacement, html, count=1)
    block = f'<script id="simulation-data" type="application/json">{json_text}</script>'
    return html.replace("</body>", f"  {block}\n</body>", 1)


def write_comparison_payload(payload: dict[str, Any], *, root: Path) -> list[Path]:
    """Write JSON and embed payload into capacity_comparison.html for file:// viewing."""
    text = json.dumps(payload, indent=2)
    size_mb = len(text) / (1024 * 1024)
    status(f"export: writing JSON ({size_mb:.1f} MB) ...")
    paths = [
        root / "data" / "simulation_results.json",
        root / "simulation_results.json",
    ]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    html_paths = [root / "capacity_comparison.html", root / "index.html"]
    existing = [p for p in html_paths if p.exists()]
    if existing:
        status(f"export: embedding payload into {len(existing)} HTML file(s) ...")
    for html_path in html_paths:
        if html_path.exists():
            html_paths_written = _embed_payload_in_html(
                html_path.read_text(encoding="utf-8"), payload
            )
            html_path.write_text(html_paths_written, encoding="utf-8")
            paths.append(html_path)
    return paths
