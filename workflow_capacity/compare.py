"""Compare capacity configurations and build Sankey flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from workflow_capacity.config import PoolConfig
from workflow_capacity.metrics import (
    ROLL_OUTS,
    aggregate_p90,
    comparison_table,
    scenario_metrics,
)
from workflow_capacity.pr_check import PrCheckRun, build_pr_check_runs
from workflow_capacity.simulate import run_pair


@dataclass
class ConfigComparison:
    config: PoolConfig
    rollout_key: str
    rollout_label: str
    baseline_rows: list
    parallel_rows: list
    base_agg: dict
    par_agg: dict
    table: list[dict[str, Any]]


def evaluate_config(
    jobs: list[dict[str, Any]],
    pr_runs: list[PrCheckRun],
    config: PoolConfig,
    *,
    rollout_key: str = "all",
    rollout_label: str = "all eligible",
    shard_eligible: Callable[[PrCheckRun], bool] | None = None,
    peak_hours: list[int] | None = None,
) -> ConfigComparison:
    baseline, parallel = run_pair(jobs, pr_runs, config, shard_eligible=shard_eligible)
    base_rows, par_rows = scenario_metrics(
        baseline, parallel, pr_runs, shard_eligible=shard_eligible
    )
    base_agg = aggregate_p90(base_rows)
    par_agg = aggregate_p90(par_rows)
    hours = peak_hours if peak_hours is not None else list(range(24))
    table = comparison_table(base_agg, par_agg, hours=hours)
    return ConfigComparison(
        config=config,
        rollout_key=rollout_key,
        rollout_label=rollout_label,
        baseline_rows=base_rows,
        parallel_rows=par_rows,
        base_agg=base_agg,
        par_agg=par_agg,
        table=table,
    )


def evaluate_matrix(
    jobs: list[dict[str, Any]],
    configs: list[PoolConfig],
    *,
    classify: bool = True,
    peak_hours: list[int] | None = None,
) -> list[ConfigComparison]:
    pr_runs = build_pr_check_runs(jobs, classify=classify)
    results: list[ConfigComparison] = []
    for config in configs:
        for rollout_key, rollout_label, shard_eligible in ROLL_OUTS:
            results.append(
                evaluate_config(
                    jobs,
                    pr_runs,
                    config,
                    rollout_key=rollout_key,
                    rollout_label=rollout_label,
                    shard_eligible=shard_eligible,
                    peak_hours=peak_hours,
                )
            )
    return results


def results_to_dataframe(results: list[ConfigComparison]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in results:
        for row in item.table:
            rows.append(
                {
                    "config": item.config.name,
                    "rollout": item.rollout_label,
                    "vm_budget": round(item.config.max_instances_budget(), 1),
                    "vcpu_quota": int(item.config.quotas["vcpu"]),
                    **row,
                }
            )
    return pd.DataFrame(rows)


def sankey_wait_work_flow(
  mono_wait: float,
  mono_work: float,
  shard_wait: float,
  shard_work: float,
  *,
  mono_label: str = "Монолит",
  shard_label: str = "Шардинг",
) -> dict[str, Any]:
    """Build plotly Sankey dict: how wait/work redistributes monolith -> sharding."""
    return {
        "type": "sankey",
        "orientation": "h",
        "node": {
            "label": [
                f"{mono_label} wait",
                f"{mono_label} work",
                f"{shard_label} wait",
                f"{shard_label} work",
            ],
            "color": ["#e45756", "#4c78a8", "#f58518", "#72b7b2"],
        },
        "link": {
            "source": [0, 1, 0, 1],
            "target": [2, 3, 3, 2],
            "value": [
                max(shard_wait, 0.1),
                max(shard_work, 0.1),
                max(mono_wait - shard_wait, 0.0) if mono_wait > shard_wait else 0.0,
                max(mono_work - shard_work, 0.0) if mono_work > shard_work else 0.0,
            ],
        },
    }


def sankey_compare_configs(
    left: ConfigComparison,
    right: ConfigComparison,
    *,
    hour: int | None = None,
    d_key: str = "all",
) -> dict[str, Any]:
    """Sankey comparing total time split between two capacity configs (sharding path)."""
    key = (hour, d_key)
    lp = left.par_agg.get(key) or left.par_agg.get((None, d_key), {})
    rp = right.par_agg.get(key) or right.par_agg.get((None, d_key), {})
    lw = lp.get("wait_p90") or 0.0
    lr = lp.get("work_p90") or 0.0
    rw = rp.get("wait_p90") or 0.0
    rr = rp.get("work_p90") or 0.0
    return {
        "type": "sankey",
        "node": {
            "label": [
                f"{left.config.name} wait",
                f"{left.config.name} work",
                f"{right.config.name} wait",
                f"{right.config.name} work",
            ],
        },
        "link": {
            "source": [0, 1],
            "target": [2, 3],
            "value": [max(lw, 0.1), max(lr, 0.1)],
        },
        "meta": {
            "right_wait": rw,
            "right_work": rr,
            "right_total": (rp.get("total_p90") or 0.0),
            "left_total": (lp.get("total_p90") or 0.0),
        },
    }
