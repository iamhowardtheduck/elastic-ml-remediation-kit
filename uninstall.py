#!/usr/bin/env python3
"""
Remove what install.py created. Same configuration as install.py.

  python3 uninstall.py --dry-run        list what would be removed
  python3 uninstall.py                  remove rules, agent, skills, tools, workflows
  python3 uninstall.py --delete-data    also delete the ml-remediation-log index and its pipeline

Elastic's kibana-anomaly-detection bundle (ad_* tools/workflows) and the ml-book-passages
index are never touched. Kibana Cases opened by the workflows are kept.
"""
import argparse, json, re, sys, urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from install import ROOT, Client, load_env, list_workflows  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--delete-data", action="store_true")
    a = ap.parse_args()
    c = Client(load_env())

    def do(kind, name, method, path, es=False):
        if a.dry_run:
            return print(f"  would delete {kind:<9} {name}")
        st, r = (c.es if es else c.kb)(method, path)
        print(f"  {'deleted' if st in (200, 204) else f'FAILED {st}':<12} {kind:<9} {name}")

    rule_files = list((ROOT / "ml-remediation/rules").glob("*.json")) + list((ROOT / "ingest-hardening/rules").glob("*.json"))
    for f in rule_files:
        name = json.loads(f.read_text())["name"]
        q = urllib.parse.quote(f'"{name}"')
        st, found = c.kb("GET", f"/api/alerting/rules/_find?search_fields=name&search={q}&per_page=10")
        for r in found.get("data", []):
            if r["name"] == name:
                do("rule", name, "DELETE", f"/api/alerting/rule/{r['id']}")

    do("agent", "ml-job-remediator", "DELETE", "/api/agent_builder/agents/ml-job-remediator")
    for f in (ROOT / "ml-remediation/skills").glob("*.json"):
        sid = json.loads(f.read_text())["id"]
        do("skill", sid, "DELETE", f"/api/agent_builder/skills/{sid}")
    for f in (ROOT / "ml-remediation/tools").glob("*.json"):
        tid = json.loads(f.read_text())["id"]
        do("tool", tid, "DELETE", f"/api/agent_builder/tools/{tid}")

    wf_ids = list_workflows(c)
    wf_files = list((ROOT / "ml-remediation/workflows").glob("*.yaml")) + list((ROOT / "ingest-hardening/workflows").glob("*.yaml"))
    for f in wf_files:
        name = re.search(r"^name:\s*(.+)$", f.read_text(), re.M).group(1).strip()
        if name in wf_ids:
            do("workflow", name, "DELETE", f"/api/workflows/workflow/{wf_ids[name]}")

    do("dashboard", "ml-job-health", "DELETE", "/api/dashboards/ml-job-health")

    if a.delete_data:
        do("index", "ml-remediation-log", "DELETE", "/ml-remediation-log", es=True)
        do("pipeline", "ml-remediation-log", "DELETE", "/_ingest/pipeline/ml-remediation-log", es=True)


if __name__ == "__main__":
    main()
