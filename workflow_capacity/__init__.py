"""Workflow capacity simulator — pool replay and PR-check sharding analysis."""

from workflow_capacity.cache import JobsDataset, ensure_dataset, list_datasets, load_dataset, resolve_dataset
from workflow_capacity.compare import evaluate_config, evaluate_matrix, results_to_dataframe
from workflow_capacity.config import PoolConfig

__all__ = [
    "JobsDataset",
    "PoolConfig",
    "ensure_dataset",
    "evaluate_config",
    "evaluate_matrix",
    "list_datasets",
    "load_dataset",
    "resolve_dataset",
    "results_to_dataframe",
]
