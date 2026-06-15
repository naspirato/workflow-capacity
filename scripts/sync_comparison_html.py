#!/usr/bin/env python3
"""Sync capacity_comparison.html and index.html from scripts/comparison_page.js."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JS_PATH = ROOT / "scripts" / "comparison_page.js"
HTML_PATHS = [ROOT / "capacity_comparison.html", ROOT / "index.html"]

SEC_PLAY_OLD = """    <section id="sec-play">
      <h2>Квоты</h2>
      <p class="hint">Только предрасчитанные точки sweep (по одной оси). Меняется одно измерение — остальные на current.</p>
      <div id="sliders"></div>"""

SEC_PLAY_NEW = """    <section id="sec-play">
      <h2>Квоты</h2>
      <p class="hint">Совместное масштабирование instances, vCPU, RAM и SSD. Только предрасчитанные точки.</p>
      <p class="hint" id="capacity-line"></p>
      <div id="sliders"></div>
      <div class="quota-grid" id="quota-grid"></div>
      <div id="load-slider" class="load-section"></div>"""

CSS_OLD = "    .chart-panel svg { width: 100%; height: auto; display: block; }\n  </style>"
CSS_NEW = """    .chart-panel svg { width: 100%; height: auto; display: block; }
    .quota-grid {
      display: flex; flex-wrap: wrap; gap: 0.35rem 1.25rem;
      font-size: 0.82rem; color: var(--muted); margin: 0.35rem 0 0.75rem;
    }
    .load-section {
      margin: 0.5rem 0 1rem;
      padding: 0.75rem 0 0;
      border-top: 1px solid var(--border);
    }
    .load-section .subhead {
      margin: 0 0 0.35rem;
      font-size: 0.95rem;
      font-weight: 600;
    }
    .table-scroll { overflow-x: auto; max-height: 28rem; overflow-y: auto; margin-top: 0.35rem; }
    .table-scroll table { font-size: 0.84rem; }
    tr.row-peak td { background: rgba(240, 180, 60, 0.06); }
    tr.row-sparse td { opacity: 0.85; }
    .chart-legend .swatch {
      display: inline-block; width: 11px; height: 9px; margin: 0 3px 0 8px;
      vertical-align: middle; border-radius: 2px;
    }
    .chart-legend .swatch:first-child { margin-left: 0; }
    .chart-legend .sw-wait { background: rgba(240, 113, 120, 0.75); }
    .chart-legend .sw-mono { background: rgba(79, 140, 255, 0.7); }
    .chart-legend .sw-shard { background: rgba(62, 207, 142, 0.7); }
    #hour-dgroup-table.matrix-table { font-size: 0.8rem; }
    #hour-dgroup-table.matrix-table th.sub { font-size: 0.72rem; font-weight: 500; }
    #hour-dgroup-table .hdr-mono { background: rgba(76, 120, 168, 0.15); }
    #hour-dgroup-table .hdr-shard { background: rgba(114, 183, 178, 0.15); }
    #hour-dgroup-table .hdr-delta { background: rgba(240, 180, 60, 0.08); }
    #hour-dgroup-table .col-dg-0 { background: rgba(100, 130, 190, 0.11); }
    #hour-dgroup-table .col-dg-1 { background: rgba(90, 150, 130, 0.1); }
    #hour-dgroup-table .col-dg-2 { background: rgba(170, 140, 90, 0.1); }
    #hour-dgroup-table .col-dg-3 { background: rgba(150, 110, 170, 0.1); }
    #hour-dgroup-table .col-dg-total { background: rgba(220, 190, 90, 0.14); font-weight: 600; }
    #hour-dgroup-table th.sub.col-dg-0,
    #hour-dgroup-table th.sub.col-dg-1,
    #hour-dgroup-table th.sub.col-dg-2,
    #hour-dgroup-table th.sub.col-dg-3,
    #hour-dgroup-table th.sub.col-dg-total { background-blend-mode: multiply; }
    #hour-dgroup-table tr.row-peak td.col-dg-0,
    #hour-dgroup-table tr.row-peak td.col-dg-1,
    #hour-dgroup-table tr.row-peak td.col-dg-2,
    #hour-dgroup-table tr.row-peak td.col-dg-3,
    #hour-dgroup-table tr.row-peak td.col-dg-total { box-shadow: inset 0 0 0 9999px rgba(240, 180, 60, 0.06); }
    #hour-dgroup-table tr.row-chart-selected td.col-dg-0,
    #hour-dgroup-table tr.row-chart-selected td.col-dg-1,
    #hour-dgroup-table tr.row-chart-selected td.col-dg-2,
    #hour-dgroup-table tr.row-chart-selected td.col-dg-3,
    #hour-dgroup-table tr.row-chart-selected td.col-dg-total { box-shadow: inset 0 0 0 1px rgba(79, 140, 255, 0.55), inset 0 0 0 9999px rgba(79, 140, 255, 0.06); }
    .chart-legend .sw-dg-0 { background: rgba(100, 130, 190, 0.55); }
    .chart-legend .sw-dg-1 { background: rgba(90, 150, 130, 0.55); }
    .chart-legend .sw-dg-2 { background: rgba(170, 140, 90, 0.55); }
    .chart-legend .sw-dg-3 { background: rgba(150, 110, 170, 0.55); }
    .chart-legend .sw-dg-total { background: rgba(220, 190, 90, 0.65); }
    #hour-dgroup-table tr.row-total th,
    #hour-dgroup-table tr.row-total td { border-top: 2px solid var(--border); font-weight: 600; }
    #hour-dgroup-table td.cell-sparse { box-shadow: inset 0 0 0 1px rgba(240, 180, 60, 0.55); }
    #hour-dgroup-table .cell-sparse-demo { padding: 0 0.35rem; border-radius: 3px; }
    #scenario-table.scenario-table { font-size: 0.82rem; }
    #scenario-table.scenario-table th.col-mono { background: rgba(76, 120, 168, 0.12); }
    #scenario-table.scenario-table th.col-shard { background: rgba(114, 183, 178, 0.12); }
    #scenario-table.scenario-table td.col-mono { background: rgba(76, 120, 168, 0.05); }
    #scenario-table.scenario-table td.col-shard { background: rgba(114, 183, 178, 0.05); }
  </style>"""

SCENARIO_TABLE_OLD = """      <table id="scenario-table">
        <thead>
          <tr>
            <th></th>
            <th>Total</th>
            <th>Ожидание</th>
            <th>Выполнение</th>
            <th>Лимит / vs монолит</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>"""

SCENARIO_TABLE_NEW = """      <table id="scenario-table" class="scenario-table">
        <thead>
          <tr>
            <th rowspan="2">Сценарий</th>
            <th rowspan="2">VM budget</th>
            <th rowspan="2">Лимит</th>
            <th colspan="3">Монолит</th>
            <th colspan="3">Sharding</th>
            <th rowspan="2">Δ total<br><span style="font-weight:400;text-transform:none">shard − mono</span></th>
            <th rowspan="2">Sharding vs<br><span style="font-weight:400;text-transform:none">mono@current</span></th>
          </tr>
          <tr>
            <th class="col-mono">Total</th>
            <th class="col-mono">Wait</th>
            <th class="col-mono">Work</th>
            <th class="col-shard">Total</th>
            <th class="col-shard">Wait</th>
            <th class="col-shard">Work</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>"""

HOUR_DGROUP_OLD = """      <h2 style="margin-top:1.25rem">По размеру PR (current)</h2>
      <table id="dgroup-table">
        <thead>
          <tr>
            <th>Группа D</th>
            <th>Монолит</th>
            <th>Sharding</th>
            <th>Δ total</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </section>"""

HOUR_DGROUP_NEW = """      <h2 style="margin-top:1.25rem">По часу × размер PR</h2>
      <p class="hint" id="hour-dgroup-subtitle">current · будни UTC</p>
      <p class="hint" id="hour-dgroup-legend"></p>
      <div class="pct-bar">
        <label><input type="checkbox" id="hour-dgroup-filter" checked /></label>
      </div>
      <div class="table-scroll">
        <table id="hour-dgroup-table" class="matrix-table">
          <thead id="hour-dgroup-thead"></thead>
          <tbody></tbody>
          <tfoot id="hour-dgroup-tfoot"></tfoot>
        </table>
      </div>
    </section>"""

MATRIX_PATCH_OLD = """      <h2 style="margin-top:1.25rem">По размеру PR (current, все часы)</h2>
      <table id="dgroup-table">
        <thead>
          <tr>
            <th>Группа D</th>
            <th>Монолит</th>
            <th>Sharding</th>
            <th>Δ total</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>

      <h2 style="margin-top:1.25rem">По часу × размер PR</h2>
      <p class="hint" id="hour-dgroup-subtitle">current · будни UTC</p>
      <div class="pct-bar">
        <label><input type="checkbox" id="hour-dgroup-filter" checked /></label>
      </div>
      <div class="table-scroll">
        <table id="hour-dgroup-table">
          <thead>
            <tr>
              <th>Час</th>
              <th>D</th>
              <th>n</th>
              <th>Монолит</th>
              <th>Sharding</th>
              <th>Δ total</th>
              <th>Ожидание</th>
              <th>Выполнение</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>"""

CHART_PATCH_OLD = """      <p class="hint" id="play-hint"></p>
      <div class="chart-grid">
        <div class="chart-panel">
          <div class="chart-title">Монолит · duration 00–23 UTC</div>"""

CHART_PATCH_NEW = """      <p class="hint" id="play-hint"></p>
      <p class="hint chart-legend" id="chart-legend-hint"></p>
      <div class="chart-grid">
        <div class="chart-panel">
          <div class="chart-title" id="chart-title-mono">Монолит</div>"""

CHART_TITLE_SHARD_OLD = '<div class="chart-title">Sharding · duration 00–23 UTC</div>'
CHART_TITLE_SHARD_NEW = '<div class="chart-title" id="chart-title-shard">Sharding</div>'


def sync_html(path: Path, js: str) -> None:
    text = path.read_text(encoding="utf-8")
    sim_match = re.search(
        r'<script id="simulation-data"[^>]*>.*?</script>\s*',
        text,
        flags=re.DOTALL,
    )
    sim_block = sim_match.group(0) if sim_match else ""
    text = text.replace(SEC_PLAY_OLD, SEC_PLAY_NEW)
    if SCENARIO_TABLE_OLD in text:
        text = text.replace(SCENARIO_TABLE_OLD, SCENARIO_TABLE_NEW)
    text = text.replace(CSS_OLD, CSS_NEW)
    if MATRIX_PATCH_OLD in text:
        text = text.replace(MATRIX_PATCH_OLD, HOUR_DGROUP_NEW)
    elif HOUR_DGROUP_OLD in text:
        text = text.replace(HOUR_DGROUP_OLD, HOUR_DGROUP_NEW)
    elif "hour-dgroup-table" not in text:
        text = text.replace(
            "      </table>\n    </section>\n\n    <section id=\"sec-play\">",
            HOUR_DGROUP_NEW.replace("    </section>\n\n", "    </section>\n\n    <section id=\"sec-play\">").split("<section id=\"sec-play\">")[0]
            + "\n    <section id=\"sec-play\">",
            1,
        )
    if CHART_PATCH_OLD in text:
        text = text.replace(CHART_PATCH_OLD, CHART_PATCH_NEW)
    text = text.replace(CHART_TITLE_SHARD_OLD, CHART_TITLE_SHARD_NEW)
    text = re.sub(
        r"<script id=\"simulation-data\"[^>]*>.*?</script>\s*",
        "",
        text,
        count=1,
        flags=re.DOTALL,
    )
    text = re.sub(
        r"<script>\s*let DATA = null;.*?\n\s*init\(\);\s*\n\s*</script>",
        f"<script>\n{js}\n  </script>",
        text,
        count=1,
        flags=re.DOTALL,
    )
    if sim_block and "simulation-data" not in text:
        text = text.replace("</body>", f"  {sim_block}</body>", 1)
    path.write_text(text, encoding="utf-8")


def main() -> int:
    js = JS_PATH.read_text(encoding="utf-8")
    for path in HTML_PATHS:
        if not path.exists():
            continue
        sync_html(path, js)
        print(f"synced {path.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
