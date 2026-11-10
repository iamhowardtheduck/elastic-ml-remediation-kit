# Runbook: ML datafeeds missing documents → ingest backlog

Symptom: many anomaly detection jobs log *"Datafeed has missed N documents due to ingest latency"*.
Raising `query_delay` hides a steady lag of a minute or two. Bursts of hours mean a backlog upstream.
Run these ES|QL queries in order. Each answers one question.

## 1. Is it real ingest lag, and where?
```esql
FROM logs-*,metrics-apm* | WHERE @timestamp > NOW() - 24 hours AND event.ingested IS NOT NULL
| SAMPLE 0.01
| EVAL lag_s = DATE_DIFF("seconds", @timestamp, event.ingested)
| STATS n = COUNT(*), p50 = PERCENTILE(lag_s, 50), p95 = PERCENTILE(lag_s, 95), max_lag = MAX(lag_s) BY data_stream.dataset
| WHERE n > 50 | SORT p95 DESC | LIMIT 15
```
A low p50 with a huge p95 means bursts, not steady lag. A large dataset that's mostly on time can hide the rest, so look per dataset.

## 2. When did it start and stop? (10-minute buckets of event time)
```esql
FROM logs-* | WHERE @timestamp >= "<start>" AND @timestamp < "<end>" AND event.ingested IS NOT NULL
| SAMPLE 0.02
| EVAL lag_s = DATE_DIFF("seconds", @timestamp, event.ingested), t = DATE_TRUNC(10 minutes, @timestamp)
| STATS p50 = PERCENTILE(lag_s, 50), p95 = PERCENTILE(lag_s, 95) BY t | SORT t
```
A p95 that falls in a straight line is a backlog draining.

## 3. Which datasets were late? A shared ingest path?
```esql
FROM logs-* | WHERE @timestamp >= "<start>" AND @timestamp < "<end>" AND event.ingested IS NOT NULL
| SAMPLE 0.02 | EVAL late = CASE(DATE_DIFF("seconds", @timestamp, event.ingested) > 600, 1, 0)
| STATS total = COUNT(*), late_n = SUM(late) BY data_stream.dataset
| EVAL late_pct = ROUND(100.0 * late_n / total, 1) | WHERE late_n > 20 | SORT late_n DESC
```
If everything from one path is late (e.g. all Fleet agents → Logstash) and other paths are on time, the shared hop is the suspect.

## 4. Logstash: queue, throughput, backpressure
```esql
FROM metrics-logstash.pipeline-* | WHERE @timestamp >= "<start>" AND @timestamp < "<end>"
| EVAL t = DATE_TRUNC(30 minutes, @timestamp)
| STATS in_eps = ROUND(AVG(logstash.pipeline.total.flow.input_throughput.current)),
        out_eps = ROUND(AVG(logstash.pipeline.total.flow.output_throughput.current)),
        worker_util = ROUND(MAX(logstash.pipeline.total.flow.worker_utilization.current)),
        backpressure = ROUND(MAX(logstash.pipeline.total.flow.queue_backpressure.current), 2),
        queue_mb = ROUND(MAX(logstash.pipeline.total.queues.current_size.bytes) / 1048576.0)
  BY logstash.pipeline.name, t | SORT logstash.pipeline.name, t
```
A queue at its `queue.max_bytes` means Logstash is pushing back on agents, which then buffer and send late.

## 5. Filters or output? (cost per event per plugin)
```esql
FROM metrics-logstash.plugins-* | WHERE @timestamp >= "<start>" AND @timestamp < "<end>" AND logstash.pipeline.name == "<pipeline>"
  AND logstash.pipeline.plugin.type IN ("filter", "output")
| EVAL t = DATE_TRUNC(30 minutes, @timestamp),
       plugin = COALESCE(logstash.pipeline.plugin.filter.name, logstash.pipeline.plugin.output.name),
       ms = COALESCE(logstash.pipeline.plugin.filter.flow.worker_millis_per_event.current, logstash.pipeline.plugin.output.flow.worker_millis_per_event.current),
       r429 = TO_LONG(`logstash.pipeline.plugin.output.metrics.elasticsearch.bulk_requests.responses.429`)
| STATS ms_per_event = ROUND(AVG(ms), 3), new_429 = MAX(r429) - MIN(r429) BY plugin, t | SORT plugin, t
```
Output cost rising with no 429s means Elasticsearch accepted the writes, but slowly.

## 6. Elasticsearch: which nodes were slow to index?
```esql
FROM metrics-elasticsearch.stack_monitoring.node_stats-* | WHERE @timestamp >= "<start>" AND @timestamp < "<end>"
| EVAL t = DATE_TRUNC(30 minutes, @timestamp), rej = TO_LONG(elasticsearch.node.stats.thread_pool.write.rejected.count),
       idx_ms = TO_LONG(elasticsearch.node.stats.indices.indexing.index_time.ms), idx_n = TO_LONG(elasticsearch.node.stats.indices.indexing.index_total.count),
       io = TO_LONG(elasticsearch.node.stats.fs.io_stats.total.operations.count)
| STATS wq = MAX(elasticsearch.node.stats.thread_pool.write.queue.count), new_rej = MAX(rej) - MIN(rej),
        ms_per_doc = ROUND(TO_DOUBLE(MAX(idx_ms) - MIN(idx_ms)) / (MAX(idx_n) - MIN(idx_n)), 3), io_ops = MAX(io) - MIN(io)
  BY elasticsearch.node.name, t | SORT elasticsearch.node.name, t
```
**Identical `io_ops` on two nodes means they share a disk** (two nodes per host). One slow node slows every bulk request, because each request waits for its slowest shard.

## 7. What extra work hit the hot tier? (ILM and shard recoveries)
```esql
FROM ilm-history-* | WHERE @timestamp >= "<start>" AND @timestamp < "<end>"
| EVAL t = DATE_TRUNC(30 minutes, @timestamp)
| STATS n = COUNT(*), indices = COUNT_DISTINCT(index) BY t, state.phase, state.action | SORT t, n DESC
```
```esql
FROM metrics-elasticsearch.stack_monitoring.index_recovery-* | WHERE @timestamp >= "<start>" AND @timestamp < "<end>"
| EVAL t = DATE_TRUNC(30 minutes, @timestamp)
| STATS recoveries = COUNT_DISTINCT(elasticsearch.index.name) BY t, elasticsearch.index.recovery.type,
        elasticsearch.index.recovery.source.name, elasticsearch.index.recovery.target.name | SORT t, recoveries DESC
```
Dozens of rollovers in one half hour, plus warm `migrate`/`forcemerge` and hot↔hot PEER recoveries, means **synchronized rollover**. Check it with `python3 stagger_rollovers.py --policy <policy>`.

ES|QL gotchas: `first` and `last` are reserved words, so don't use them as aliases. Counter fields need `TO_LONG()` before `MAX`/`MIN`. Field names ending in a number need backticks.

## Fixes, cheapest first

1. **Throttle recoveries** so shard moves can't take over hot disks. Check for aggressive *transient* settings left over from a past recovery:
   ```
   GET _cluster/settings?include_defaults=true&filter_path=*.cluster.routing.allocation.node_concurrent_recoveries,*.cluster.routing.allocation.cluster_concurrent_rebalance,*.indices.recovery.max_bytes_per_sec
   PUT _cluster/settings
   {"transient": {"cluster.routing.allocation.node_concurrent_recoveries": null, "cluster.routing.allocation.cluster_concurrent_rebalance": null, "indices.recovery.max_bytes_per_sec": null},
    "persistent": {"cluster.routing.allocation.node_concurrent_recoveries": 2, "cluster.routing.allocation.cluster_concurrent_rebalance": 2, "indices.recovery.max_bytes_per_sec": "100mb"}}
   ```
2. **Spread out synchronized rollovers:** `stagger_rollovers.py --policy <p> --groups 3 --spacing 5h --apply`.
3. **Use larger Logstash bulk batches**, e.g. `pipeline.batch.size` 250 → 750–1000. Check JVM heap headroom first. Centrally managed pipelines: `PUT /api/logstash/pipeline/<id>` with `{pipeline, settings}`.
4. **Alert before backpressure:** the kit's *Logstash persistent queue backing up* rule (over 5 GB or 25%).
5. **Capacity:** give each hot node its own disk. Two nodes on one volume halve the I/O each can use.
