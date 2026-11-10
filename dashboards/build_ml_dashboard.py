#!/usr/bin/env python3
"""
Generates dashboards/ml-job-health.dashboard.json - the "ML Job Health & Remediation" Kibana dashboard.

  python3 dashboards/build_ml_dashboard.py            # (re)write the JSON
  python3 dashboards/build_ml_dashboard.py --test     # run every panel query against the cluster (24h window)

install.py publishes the JSON with PUT /api/dashboards/ml-job-health (upsert, stable id).
Data sources: stack-monitoring ML job metrics, .ml-notifications-*, .ml-anomalies-*,
the alerting index, ml-remediation-log and Logstash pipeline metrics.
"""
import json, re, sys
from pathlib import Path

OUT = Path(__file__).resolve().parent / "ml-job-health.dashboard.json"
T = "?_tstart"; TE = "?_tend"
B = f"BUCKET(@timestamp, 48, {T}, {TE})"
MLJOB = "metrics-elasticsearch.stack_monitoring.ml_job-*"
RULES = '"ML Notifications - Delayed Data Detected", "ML Notifications - Job/Datafeed Errors", "Logstash persistent queue backing up"'
KIND = ('CASE(message LIKE "*ingest latency*", "Delayed data", message LIKE "*no data*", "No data", '
        'message LIKE "*extracting data*", "Extraction error", message LIKE "*No node found*", "No node to start", '
        'message LIKE "*memory*", "Memory", message LIKE "*flushing*", "Flush error", "Other")')
LATEST_STATE = (f"FROM {MLJOB} | WHERE @timestamp > NOW() - 10 minutes "
                "| STATS state = LAST(elasticsearch.ml.job.state, @timestamp), mem = LAST(elasticsearch.ml.job.model_size.memory_status, @timestamp) "
                "BY job = elasticsearch.ml.job.id")
# The datafeed repeats its warning for the same bucket on every check: keep the max per job+bucket, then sum.
MISSED = ('| GROK message "Datafeed has missed %{DATA:missed_raw} documents due to ingest latency, latest bucket with missing data is \\\\[%{DATA:bucket}\\\\]" '
          '| EVAL missed_n = TO_LONG(REPLACE(missed_raw, ",", "")) '
          '| STATS missed = MAX(missed_n) BY job_id, bucket')
NO_TITLES = {"x": {"title": {"visible": False}}, "y": {"title": {"visible": False}}}
TIME_AXIS = {"x": {"title": {"visible": False}, "scale": "temporal", "domain": {"type": "fit", "rounding": False}},
             "y": {"title": {"visible": False}}}


def metric(pid, x, w, query, col, fmt=None, secondary=None):
    m = [{"type": "primary", "column": col, **({"format": fmt} if fmt else {})}]
    if secondary:
        m.append({"type": "secondary", "column": secondary})
    return {"type": "vis", "id": pid, "grid": {"x": x, "y": 0, "w": w, "h": 5},
            "config": {"title": "", "type": "metric", "data_source": {"type": "esql", "query": query}, "metrics": m}}


def xy(pid, grid, title, layer_type, query, x_col, ys, breakdown=None, temporal=True, x_label="Time"):
    layer = {"type": layer_type, "data_source": {"type": "esql", "query": query},
             "x": {"column": x_col, **({"label": x_label} if temporal else {})}, "y": ys}
    if breakdown:
        layer["breakdown_by"] = {"column": breakdown}
    return {"type": "vis", "id": pid, "grid": grid,
            "config": {"title": title, "type": "xy", "axis": TIME_AXIS if temporal else NO_TITLES, "layers": [layer]}}


def table(pid, grid, title, query, rows, metrics):
    return {"type": "vis", "id": pid, "grid": grid,
            "config": {"title": title, "type": "data_table", "data_source": {"type": "esql", "query": query},
                       "rows": [{"column": r} for r in rows], "metrics": [{"column": m} for m in metrics]}}


panels = [
    # ---- row 0: KPIs (h=5) -------------------------------------------------------------------------------
    metric("kpi-opened", 0, 7, LATEST_STATE + ' | STATS `Jobs opened` = SUM(CASE(state == "opened", 1, 0)), `of jobs` = COUNT(*)',
           "Jobs opened", secondary="of jobs"),
    metric("kpi-failed", 7, 7, LATEST_STATE + ' | STATS `Failed jobs` = SUM(CASE(state == "failed", 1, 0))', "Failed jobs"),
    metric("kpi-memory", 14, 7, LATEST_STATE + ' | STATS `At memory limit` = SUM(CASE(mem IN ("soft_limit", "hard_limit"), 1, 0))',
           "At memory limit"),
    metric("kpi-alerts", 21, 7, f'FROM .alerts-stack.alerts-default | WHERE kibana.alert.status == "active" AND kibana.alert.rule.name IN ({RULES}) '
           '| STATS `Active alerts` = COUNT_DISTINCT(kibana.alert.uuid)', "Active alerts"),
    metric("kpi-missed", 28, 7, f'FROM .ml-notifications-* | WHERE timestamp >= {T} AND timestamp <= {TE} AND job_type == "anomaly_detector" '
           f'AND message LIKE "*ingest latency*" {MISSED} | STATS `Docs missed` = SUM(missed)', "Docs missed",
           fmt={"type": "number", "decimals": 0, "compact": True}),
    metric("kpi-fixes", 35, 7, f'FROM ml-remediation-log | WHERE @timestamp >= {T} AND @timestamp <= {TE} AND outcome == "applied" '
           '| STATS `Auto-fixes applied` = COUNT(*)', "Auto-fixes applied"),
    metric("kpi-escalations", 42, 6, f'FROM ml-remediation-log | WHERE @timestamp >= {T} AND @timestamp <= {TE} AND outcome IN ("escalated", "failed") '
           '| STATS `Escalations` = COUNT(*)', "Escalations"),

    # ---- row 5: health over time (h=10) ------------------------------------------------------------------
    xy("job-state", {"x": 0, "y": 5, "w": 24, "h": 10}, "Anomaly detection jobs by state", "bar_stacked",
       f"FROM {MLJOB} | WHERE @timestamp >= {T} AND @timestamp <= {TE} "
       f"| STATS state = LAST(elasticsearch.ml.job.state, @timestamp) BY job = elasticsearch.ml.job.id, t = {B} "
       "| STATS jobs = COUNT(*) BY t, state",
       "t", [{"column": "jobs"}], breakdown="state"),
    xy("problems-by-type", {"x": 24, "y": 5, "w": 24, "h": 10}, "Job warnings and errors by type", "bar_stacked",
       f'FROM .ml-notifications-* | WHERE timestamp >= {T} AND timestamp <= {TE} AND job_type == "anomaly_detector" '
       f'AND level IN ("warning", "error") | EVAL type = {KIND} '
       f"| STATS messages = COUNT(*) BY t = BUCKET(timestamp, 48, {T}, {TE}), type",
       "t", [{"column": "messages"}], breakdown="type"),

    # ---- row 15: performance (h=10) ----------------------------------------------------------------------
    xy("missed-by-job", {"x": 0, "y": 15, "w": 16, "h": 10}, "Documents missed (ingest latency) - top jobs", "bar_horizontal",
       f'FROM .ml-notifications-* | WHERE timestamp >= {T} AND timestamp <= {TE} AND message LIKE "*ingest latency*" {MISSED} '
       "| STATS missed = SUM(missed) BY job_id | SORT missed DESC | LIMIT 10",
       "job_id", [{"column": "missed", "format": {"type": "number", "decimals": 0, "compact": True}}], temporal=False),
    xy("bucket-latency", {"x": 16, "y": 15, "w": 16, "h": 10}, "Bucket processing time (ms, all jobs)", "line",
       f'FROM .ml-anomalies-* | WHERE result_type == "bucket" AND timestamp >= {T} AND timestamp <= {TE} '
       f"| STATS p50 = PERCENTILE(processing_time_ms, 50), p95 = PERCENTILE(processing_time_ms, 95) BY t = BUCKET(timestamp, 48, {T}, {TE})",
       "t", [{"column": "p50"}, {"column": "p95"}]),
    xy("records-processed", {"x": 32, "y": 15, "w": 16, "h": 10}, "Records analyzed (all jobs)", "area",
       f"FROM {MLJOB} | WHERE @timestamp >= {T} AND @timestamp <= {TE} "
       f"| STATS hi = MAX(elasticsearch.ml.job.data_counts.processed_record_count), lo = MIN(elasticsearch.ml.job.data_counts.processed_record_count) "
       f"BY job = elasticsearch.ml.job.id, t = {B} | EVAL delta = CASE(hi >= lo, hi - lo, 0) | STATS records = SUM(delta) BY t",
       "t", [{"column": "records", "format": {"type": "number", "decimals": 1, "compact": True}}]),

    # ---- row 25: alerting & automation (h=10) ------------------------------------------------------------
    xy("alerts-by-rule", {"x": 0, "y": 25, "w": 16, "h": 10}, "Remediation alerts raised, by rule", "bar_stacked",
       f"FROM .alerts-stack.alerts-default | WHERE kibana.alert.rule.name IN ({RULES}) "
       f"AND kibana.alert.start >= {T} AND kibana.alert.start <= {TE} "
       f"| STATS alerts = COUNT_DISTINCT(kibana.alert.uuid) BY t = BUCKET(kibana.alert.start, 48, {T}, {TE}), rule = kibana.alert.rule.name",
       "t", [{"column": "alerts"}], breakdown="rule"),
    xy("actions-by-outcome", {"x": 16, "y": 25, "w": 16, "h": 10}, "Remediation actions, by outcome", "bar_stacked",
       f'FROM ml-remediation-log | WHERE @timestamp >= {T} AND @timestamp <= {TE} AND outcome != "no_action" '
       f"| STATS actions = COUNT(*) BY t = {B}, outcome",
       "t", [{"column": "actions"}], breakdown="outcome"),
    xy("logstash-queue", {"x": 32, "y": 25, "w": 16, "h": 10}, "Logstash persistent queue, by pipeline", "line",
       f'FROM metrics-logstash.pipeline-* | WHERE @timestamp >= {T} AND @timestamp <= {TE} AND logstash.pipeline.total.queues.type == "persisted" '
       f"| STATS queue = MAX(logstash.pipeline.total.queues.current_size.bytes) BY t = {B}, pipeline = logstash.pipeline.name",
       "t", [{"column": "queue", "format": {"type": "bytes", "decimals": 1}}], breakdown="pipeline"),

    # ---- row 35: per-job detail (h=12) -------------------------------------------------------------------
    table("memory-table", {"x": 0, "y": 35, "w": 24, "h": 12}, "Model memory by job (latest)",
          f'FROM .ml-anomalies-* | WHERE result_type == "model_size_stats" AND timestamp >= {T} AND timestamp <= {TE} '
          "| STATS model_bytes = LAST(model_bytes, timestamp), limit_raw = LAST(model_bytes_memory_limit, timestamp), "
          "memory_status = LAST(memory_status, timestamp), by_fields = LAST(total_by_field_count, timestamp) BY job_id "
          "| EVAL limit_bytes = TO_DOUBLE(limit_raw), `Model MB` = ROUND(model_bytes / 1048576.0, 1), `Limit MB` = ROUND(limit_bytes / 1048576.0), "
          "`% of limit` = ROUND(100.0 * model_bytes / limit_bytes, 1) "
          "| SORT `% of limit` DESC | KEEP job_id, memory_status, `Model MB`, `Limit MB`, `% of limit`, by_fields | LIMIT 50",
          ["job_id", "memory_status"], ["Model MB", "Limit MB", "% of limit", "by_fields"]),
    table("problems-table", {"x": 24, "y": 35, "w": 24, "h": 12}, "Jobs with warnings or errors",
          f'FROM .ml-notifications-* | WHERE timestamp >= {T} AND timestamp <= {TE} AND job_type == "anomaly_detector" '
          f'AND level IN ("warning", "error") | EVAL type = {KIND} '
          '| STATS Warnings = SUM(CASE(level == "warning", 1, 0)), Errors = SUM(CASE(level == "error", 1, 0)), '
          "`Latest problem` = LAST(type, timestamp), `Last seen` = MAX(timestamp) BY job_id "
          "| SORT Errors DESC, Warnings DESC | LIMIT 50",
          ["job_id", "Latest problem"], ["Errors", "Warnings", "Last seen"]),

    # ---- row 47: audit trail (h=10) ----------------------------------------------------------------------
    table("remediation-log", {"x": 0, "y": 47, "w": 48, "h": 10}, "Remediation actions by job (last 7 days)",
          f'FROM ml-remediation-log | WHERE @timestamp >= {TE} - 7 days AND outcome != "no_action" '
          "| STATS Actions = COUNT(*), `Last action` = MAX(@timestamp), `query_delay now` = LAST(query_delay_after, @timestamp), "
          "Case = LAST(case_id, @timestamp) BY job_id, action, outcome "
          "| SORT `Last action` DESC | LIMIT 100",
          ["job_id", "action", "outcome", "query_delay now", "Case"], ["Actions", "Last action"]),
]

dashboard = {
    "title": "ML Job Health & Remediation",
    "description": "Anomaly detection job health and performance, the alert conditions that drive automated "
                   "remediation (delayed data, job errors, Logstash backlog), and what the automation did. "
                   "Part of the Elastic ML Remediation Kit.",
    "time_range": {"from": "now-24h", "to": "now"},
    "panels": panels,
}


def test():
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from install import Client, load_env
    c = Client(load_env())
    bad = 0
    for p in panels:
        cfg = p["config"]
        srcs = [cfg["data_source"]] if "data_source" in cfg else [l["data_source"] for l in cfg["layers"]]
        for s in srcs:
            q = s["query"].replace(T, '"' + "NOW_MINUS_24H" + '"').replace(TE, '"NOW_TS"')
            q = q.replace('"NOW_MINUS_24H"', "NOW() - 24 hours").replace('"NOW_TS"', "NOW()")
            st, r = c.es("POST", "/_query", {"query": q})
            if st != 200:
                bad += 1
                print(f"FAIL {p['id']}: {json.dumps(r.get('error', r))[:300]}")
                continue
            cols = {col["name"] for col in r.get("columns", [])}
            want = set()
            if cfg["type"] == "metric":
                want = {m["column"] for m in cfg["metrics"]}
            elif cfg["type"] == "data_table":
                want = {x["column"] for x in cfg["rows"] + cfg["metrics"]}
            else:
                l = cfg["layers"][0]
                want = {l["x"]["column"], *(y["column"] for y in l["y"])} | ({l["breakdown_by"]["column"]} if "breakdown_by" in l else set())
            miss = want - cols
            rows = len(r.get("values", []))
            print(f"{'OK  ' if not miss else 'COLS'} {p['id']:<20} rows={rows:<4}" + (f" missing={miss}" if miss else "")
                  + (f"  sample={r['values'][0][:4]}" if rows and cfg['type'] == 'metric' else ""))
            bad += bool(miss)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard, indent=2) + "\n")
    print(f"wrote {OUT.name}: {len(panels)} panels")
    if "--test" in sys.argv:
        test()
