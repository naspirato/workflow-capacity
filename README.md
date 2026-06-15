# Workflow Capacity

Симулятор загрузки self-hosted runner pool и сравнение **монолит vs PR-check sharding**.

Данные с GitHub собираются **один раз** в `data/cache/` и переиспользуются при смене квот / конфигов раннеров.

## Быстрый старт

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
gh auth login   # для сбора данных

# Jupyter — интерактивный анализ
jupyter notebook notebooks/capacity_explorer.ipynb

# HTML-страница: index.html + simulation_results.json (GitHub Pages)
# Локально: python3 -m http.server → http://localhost:8000/
# file://: export с --embed-local перезаписывает capacity_comparison.html (не коммитить fat-версию)
jupyter notebook notebooks/generate_comparison_page.ipynb
```

## Структура

```
config/capacity.example.yml   # квоты, static runners, footprints
workflow_capacity/            # ядро симулятора
  config.py                   # PoolConfig + static runners
  pool.py                     # discrete-event pool
  simulate.py                 # replay monolith vs sharding
  metrics.py                  # wait/work/total по срезам
  compare.py                  # сравнение конфигов, Sankey
  cache.py                    # кэш job history по окну дат
  collect.py                  # GitHub API collector
  augment.py                  # base_ref из PR API
notebooks/capacity_explorer.ipynb
notebooks/generate_comparison_page.ipynb   # → simulation_results.json + index.html
data/cache/                   # jobs_*.json (gitignored)
```

## Конфиг capacity

- `quotas` — лимиты folder (instances, vcpu, ram_gb, nrd_ssd_gb)
- `static_runners` — зарезервированные VM (tiny-worker, analytics-cache, …)
- `footprints` — потребление на один auto-provisioned runner по label
- `reserved` — legacy aggregate reserve (если static_runners не покрывает всё)

Добавить новый тип раннера:

```yaml
static_runners:
  my-cache-node:
    count: 2
    footprint: {vcpu: 16, ram_gb: 64, nrd_ssd_gb: 500}

footprints:
  build-preset-my-new-preset:
    vcpu: 48
    ram_gb: 192
    nrd_ssd_gb: 1500
```

## Кэш истории

Файлы: `data/cache/jobs_{repo}_{since}_{until}.json`

```python
from workflow_capacity.cache import ensure_dataset, list_datasets

# первый запуск — скачает с GitHub
ds = ensure_dataset(days=14, repo="ydb-platform/ydb")

# повторные прогоны — только симуляция
ds = ensure_dataset(days=14)  # из кэша
```

Разные окна (7d, 14d, 30d) лежат **рядом** — перевыкачка не нужна при смене vCPU/RAM/instances.

При **augment** в тот же JSON пишется `pr_files`: для каждого PR — `file_count` и список путей. Правила sharded vs monolith — секция `pr_classify` в `config/capacity.example.yml` (можно менять без перевыкачки).

## CLI (опционально)

```bash
python -m workflow_capacity.collect --days 14 --output data/cache/jobs_test.json
python -m workflow_capacity.augment --data data/cache/jobs_test.json
```

## Источник

Вынесено из `ydb/ci/runner-load-simulator` (без промежуточных отчётов и графиков).

Репозиторий: https://github.com/naspirato/workflow-capacity

### Первый push (если репозиторий пустой)

```bash
git clone https://github.com/naspirato/workflow-capacity.git && cd workflow-capacity
git clone --depth 1 --branch cursor/runner-load-simulator-1a26 https://github.com/naspirato/ydb.git /tmp/ydb-wc
cp -r /tmp/ydb-wc/workflow-capacity/* . && rm -rf /tmp/ydb-wc
git add -A && git commit -m "Initial workflow-capacity import" && git push -u origin main
```
