#!/usr/bin/env python3
"""
Detect and break up synchronized ILM rollovers.

When many data streams share one ILM policy with a time-based rollover (e.g. max_age: 7d) and were
created at about the same time, they all roll over in the same minute every period - and their
previous generations migrate to warm (relocation + forcemerge) together a little later. On a busy
hot tier that burst can slow indexing enough to fill Logstash persistent queues.

  python3 stagger_rollovers.py --policy my-policy                 # report bursts (read-only)
  python3 stagger_rollovers.py --policy my-policy --groups 3 --spacing 5h --apply
      -> rolls over group A now and creates + starts a one-off Kibana workflow that rolls the
         remaining groups <spacing> apart, so next period's rollovers are spread out.

Groups are balanced by current write-index size. Only data streams whose write index was created
in the largest burst (same --window) are touched. Same configuration as install.py.
"""
import argparse, datetime as dt, json, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from install import Client, load_env  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--policy", required=True)
    ap.add_argument("--window", type=int, default=15, help="minutes: write indices created this close together form a burst")
    ap.add_argument("--min-burst", type=int, default=10)
    ap.add_argument("--groups", type=int, default=3)
    ap.add_argument("--spacing", default="5h", help="delay between groups, workflow duration syntax (e.g. 4h, 90m)")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    c = Client(load_env())

    st, r = c.es("GET", "/_data_stream?expand_wildcards=open&filter_path=data_streams.name,data_streams.ilm_policy,data_streams.indices.index_name")
    streams = [d for d in r.get("data_streams", []) if d.get("ilm_policy") == a.policy]
    if not streams:
        sys.exit(f"no data streams use ILM policy {a.policy}")
    st, cat = c.es("GET", "/_cat/indices/.ds-*?format=json&h=index,creation.date,pri.store.size&bytes=b&expand_wildcards=all")
    info = {x["index"]: x for x in cat}
    rows = []
    for d in streams:
        w = d["indices"][-1]["index_name"]
        x = info.get(w, {})
        if x.get("creation.date"):
            rows.append((d["name"], int(x["creation.date"]), int(x.get("pri.store.size") or 0)))
    rows.sort(key=lambda r: r[1])

    bursts, cur = [], []                      # cluster by creation time
    for row in rows:
        if cur and row[1] - cur[-1][1] > a.window * 60000:
            bursts.append(cur); cur = []
        cur.append(row)
    if cur:
        bursts.append(cur)
    print(f"{len(rows)} data streams on {a.policy}; write-index creation bursts (>= {a.min_burst}):")
    big = [b for b in bursts if len(b) >= a.min_burst]
    for b in big:
        t = dt.datetime.fromtimestamp(b[0][1] / 1000, dt.timezone.utc)
        print(f"  {t:%Y-%m-%d %H:%M} UTC  {len(b):>4} streams  {sum(x[2] for x in b)/1e9:7.1f} GB in write indices")
    if not big:
        return print("No synchronized burst found - nothing to do.")
    burst = max(big, key=len)

    groups = [[] for _ in range(a.groups)]
    sizes = [0] * a.groups
    for name, _, sz in sorted(burst, key=lambda r: -r[2]):
        i = sizes.index(min(sizes)); groups[i].append(name); sizes[i] += sz
    for i, g in enumerate(groups):
        print(f"  group {chr(65+i)}: {len(g):>3} streams, {sizes[i]/1e9:.1f} GB")
    if not a.apply:
        return print("\nRead-only. Re-run with --apply to roll group A now and schedule the rest.")

    for name in groups[0]:
        st, r = c.es("POST", f"/{name}/_rollover")
        print(f"  {'rolled' if r.get('rolled_over') else 'FAILED'}  {name}")
    if len(groups) == 1:
        return
    steps = []
    for i, g in enumerate(groups[1:], start=1):
        L = chr(65 + i)
        steps.append(f"  - name: wait_{L}\n    type: wait\n    with:\n      duration: {a.spacing}")
        steps.append(f"  - name: rollover_{L}\n    type: foreach\n    foreach: \"{{{{ consts.group_{L} }}}}\"\n    steps:\n"
                     f"      - name: roll_{L}\n        type: elasticsearch.request\n        with:\n          method: POST\n"
                     f"          path: \"/{{{{ foreach.item }}}}/_rollover\"\n        on-failure:\n          continue: true")
    consts = "\n".join(f"  group_{chr(65+i)}:\n" + "\n".join(f'    - "{n}"' for n in g) for i, g in enumerate(groups[1:], start=1))
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M")
    yaml = (f"name: ILM Rollover Stagger {a.policy} {stamp}\n"
            f"description: One-off - rolls the remaining groups of {a.policy} data streams {a.spacing} apart. Delete when done.\n"
            f"enabled: true\ntags: [\"ilm\", \"ingest-hardening\"]\ntriggers:\n  - type: manual\nconsts:\n{consts}\nsteps:\n" + "\n".join(steps) + "\n")
    st, r = c.kb("POST", "/api/workflows?overwrite=true", {"workflows": [{"yaml": yaml}]})
    wid = ((r.get("created") or [{}])[0]).get("id")
    if not wid:
        sys.exit(f"workflow creation failed: {json.dumps(r)[:400]}")
    st, r = c.kb("POST", f"/api/workflows/workflow/{wid}/run", {"inputs": {}})
    print(f"\nWorkflow {wid} started (execution {r.get('workflowExecutionId')}); "
          f"groups B.. roll over every {a.spacing}. Check its Executions tab in Kibana.")


if __name__ == "__main__":
    main()
