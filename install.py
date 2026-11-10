#!/usr/bin/env python3
"""
Elastic ML Remediation Kit installer (Python 3.9+, standard library only).

Installs or updates, idempotently:
  * index + ingest pipeline  ml-remediation-log
  * Kibana Workflows          (create by name, update in place with PUT - never duplicates)
  * Agent Builder tools, skills and the ml-job-remediator agent
  * Kibana alerting rules     (matched by name)
  * optional: ingest-hardening (Logstash queue backlog rule + workflow)

Modes:
  python3 install.py --dry-run     show what would be created/updated, change nothing
  python3 install.py --check       compare the package with what is deployed (read-only drift report)
  python3 install.py               install / update
Options:
  --components ml,ingest           what to install (default: ml,ingest)
  --book auto|yes|no               ml_book_search tool + ml-job-design-guide skill (default auto:
                                   only if the ml-book-passages index exists - see knowledge-base/)
  --no-rules                       install everything except alerting rules (no automation runs)

Configuration (environment variables or a .env file next to this script):
  ES_URL, KIBANA_URL             e.g. https://es.example:9200, https://kibana.example:5601
  ES_API_KEY                     base64 "id:key" API key  (or ES_USERNAME + ES_PASSWORD)
  ES_CA_CERT                     path to a CA bundle (optional)
  INSECURE=true                  skip TLS verification (not recommended)
  KIBANA_SPACE                   Kibana space id (optional, default space if unset)
"""
import argparse, base64, json, os, re, ssl, sys, urllib.error, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
AGENT_ID = "ml-job-remediator"
BOOK_INDEX = "ml-book-passages"
BOOK_TOOL, BOOK_SKILL = "ml_book_search", "ml-job-design-guide"
# Tools the skills use that come from Elastic's kibana-anomaly-detection Agent Builder bundle
BUNDLE_TOOLS = ["ad_get_jobs", "ad_get_job_messages", "ad_get_model_snapshots", "ad_ts_bucket_event_gaps",
                "ad_ts_delayed_data_annotations", "ad_ts_ingest_latency_estimate", "ad_ts_model_memory_health"]
# Order matters: a workflow may reference another one by placeholder
ML_WORKFLOWS = ["ml-ad-remediate-job.yaml", "ml-ad-health-sweep.yaml",
                "ml-job-datafeed-delayed-data-auto-remediation.yaml"]
PLACEHOLDER = re.compile(r"@@workflow:([^@]+)@@")


# ----------------------------------------------------------------------------- config / http
def load_env():
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    cfg = {k: os.environ.get(k, "") for k in
           ("ES_URL", "KIBANA_URL", "ES_API_KEY", "ES_USERNAME", "ES_PASSWORD", "ES_CA_CERT", "INSECURE", "KIBANA_SPACE")}
    missing = [k for k in ("ES_URL", "KIBANA_URL") if not cfg[k]]
    if not cfg["ES_API_KEY"] and not (cfg["ES_USERNAME"] and cfg["ES_PASSWORD"]):
        missing.append("ES_API_KEY (or ES_USERNAME + ES_PASSWORD)")
    if missing:
        sys.exit("Missing configuration: " + ", ".join(missing) + "  (see config.example.env)")
    return cfg


class Client:
    def __init__(self, cfg):
        self.cfg = cfg
        if cfg["INSECURE"].lower() == "true":
            self.ctx = ssl._create_unverified_context()
        else:
            self.ctx = ssl.create_default_context(cafile=cfg["ES_CA_CERT"] or None)
        if cfg["ES_API_KEY"]:
            self.auth = "ApiKey " + cfg["ES_API_KEY"]
        else:
            self.auth = "Basic " + base64.b64encode(f"{cfg['ES_USERNAME']}:{cfg['ES_PASSWORD']}".encode()).decode()
        space = cfg["KIBANA_SPACE"]
        self.kb_base = cfg["KIBANA_URL"].rstrip("/") + (f"/s/{space}" if space else "")
        self.es_base = cfg["ES_URL"].rstrip("/")

    def _req(self, base, method, path, body=None, kibana=False):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": self.auth, "Content-Type": "application/json"}
        if kibana:
            headers.update({"kbn-xsrf": "true", "x-elastic-internal-origin": "kibana"})
        req = urllib.request.Request(base + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=120) as r:
                raw = r.read()
                return r.status, (json.loads(raw) if raw else {})
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"error": raw.decode(errors="replace")[:500]}

    def es(self, method, path, body=None):
        return self._req(self.es_base, method, path, body)

    def kb(self, method, path, body=None):
        return self._req(self.kb_base, method, path, body, kibana=True)


# ----------------------------------------------------------------------------- reporting
class Report:
    def __init__(self, mode):
        self.mode, self.rows, self.errors = mode, [], 0

    def add(self, kind, name, status, note=""):
        self.rows.append((kind, name, status, note))
        if status in ("ERROR", "MISSING-PREREQ"):
            self.errors += 1
        print(f"  {status:<15} {kind:<9} {name}" + (f"  - {note}" if note else ""))


def norm(o):
    return json.dumps(o, sort_keys=True)


# ----------------------------------------------------------------------------- installers
def install_index(c, rep, apply):
    pipe = json.loads((ROOT / "ml-remediation/index/ml-remediation-log.pipeline.json").read_text())
    idx = json.loads((ROOT / "ml-remediation/index/ml-remediation-log.index.json").read_text())
    st, cur = c.es("GET", "/_ingest/pipeline/ml-remediation-log")
    same = st == 200 and norm(cur.get("ml-remediation-log", {}).get("processors")) == norm(pipe["processors"])
    status = "same" if same else ("update" if st == 200 else "create")
    if apply and not same:
        st2, r = c.es("PUT", "/_ingest/pipeline/ml-remediation-log", pipe)
        status = "OK" if st2 == 200 else "ERROR"
    rep.add("pipeline", "ml-remediation-log", status)
    st, _ = c.es("HEAD", "/ml-remediation-log")
    status = "exists" if st == 200 else "create"
    if apply and st != 200:
        st2, r = c.es("PUT", "/ml-remediation-log", idx)
        status = "OK" if st2 == 200 else "ERROR"
    rep.add("index", "ml-remediation-log", status)


def list_workflows(c):
    out, page = {}, 1
    while True:
        st, r = c.kb("GET", f"/api/workflows?size=100&page={page}")
        if st != 200:
            raise RuntimeError(f"cannot list workflows: {st} {r}")
        for w in r.get("results", []):
            out[w["name"]] = w["id"]
        if page * 100 >= r.get("total", 0):
            return out
        page += 1


def resolve(text, wf_ids, rep, where):
    def sub(m):
        name = m.group(1)
        if name not in wf_ids:
            rep.add("ref", name, "MISSING-PREREQ", f"workflow referenced by {where} not found")
            return m.group(0)
        return wf_ids[name]
    return PLACEHOLDER.sub(sub, text)


def install_workflow(c, rep, apply, path, wf_ids):
    raw = path.read_text()
    name = re.search(r"^name:\s*(.+)$", raw, re.M).group(1).strip()
    yaml_text = resolve(raw, wf_ids, rep, path.name)
    if name in wf_ids:
        wid = wf_ids[name]
        st, cur = c.kb("GET", f"/api/workflows/workflow/{wid}")
        if st == 200 and cur.get("yaml", "").strip() == yaml_text.strip():
            rep.add("workflow", name, "same", wid)
            return
        if not apply:
            rep.add("workflow", name, "update", wid)
            return
        st, r = c.kb("PUT", f"/api/workflows/workflow/{wid}", {"yaml": yaml_text})
        ok = st == 200 and r.get("valid", True)
        rep.add("workflow", name, "OK" if ok else "ERROR", wid if ok else json.dumps(r)[:300])
        return
    if not apply:
        rep.add("workflow", name, "create")
        wf_ids[name] = "<new:" + name + ">"
        return
    # create (POST only for names that do not exist yet - overwrite=true on an existing name duplicates)
    st, r = c.kb("POST", "/api/workflows?overwrite=true", {"workflows": [{"yaml": yaml_text}]})
    created = (r.get("created") or [{}])[0] if st == 200 else {}
    if created.get("id"):
        wf_ids[name] = created["id"]
        rep.add("workflow", name, "OK", created["id"])
    else:
        rep.add("workflow", name, "ERROR", json.dumps(r)[:300])


def install_tool(c, rep, apply, tool):
    st, cur = c.kb("GET", f"/api/agent_builder/tools/{tool['id']}")
    body = {k: tool[k] for k in ("description", "configuration", "tags") if k in tool}
    if st == 200:
        same = all(norm(cur.get(k)) == norm(v) for k, v in body.items()) and \
            norm(cur.get("confirmation")) == norm(tool.get("confirmation", cur.get("confirmation")))
        if same:
            return rep.add("tool", tool["id"], "same")
        if not apply:
            return rep.add("tool", tool["id"], "update")
        if "confirmation" in tool:
            body["confirmation"] = tool["confirmation"]
        st, r = c.kb("PUT", f"/api/agent_builder/tools/{tool['id']}", body)
        if st == 400 and "confirmation" in body:          # older builds: confirmation not updatable
            body.pop("confirmation")
            st, r = c.kb("PUT", f"/api/agent_builder/tools/{tool['id']}", body)
        return rep.add("tool", tool["id"], "OK" if st == 200 else "ERROR", "" if st == 200 else json.dumps(r)[:300])
    if not apply:
        return rep.add("tool", tool["id"], "create")
    st, r = c.kb("POST", "/api/agent_builder/tools", tool)
    rep.add("tool", tool["id"], "OK" if st == 200 else "ERROR", "" if st == 200 else json.dumps(r)[:300])


def install_skill(c, rep, apply, skill):
    st, cur = c.kb("GET", f"/api/agent_builder/skills/{skill['id']}")
    body = {k: skill[k] for k in ("name", "description", "content", "tool_ids")}
    if st == 200:
        if all(norm(cur.get(k)) == norm(v) for k, v in body.items()):
            return rep.add("skill", skill["id"], "same")
        if not apply:
            return rep.add("skill", skill["id"], "update")
        st, r = c.kb("PUT", f"/api/agent_builder/skills/{skill['id']}", body)
    else:
        if not apply:
            return rep.add("skill", skill["id"], "create")
        st, r = c.kb("POST", "/api/agent_builder/skills", skill)
    rep.add("skill", skill["id"], "OK" if st == 200 else "ERROR", "" if st == 200 else json.dumps(r)[:300])


def install_agent(c, rep, apply, agent):
    st, cur = c.kb("GET", f"/api/agent_builder/agents/{agent['id']}")
    body = {k: agent[k] for k in ("name", "description", "labels", "avatar_color", "avatar_symbol", "configuration")}
    if st == 200:
        cc, nc = cur.get("configuration", {}), agent["configuration"]
        if cc.get("instructions") == nc["instructions"] and sorted(cc.get("skill_ids") or []) == sorted(nc["skill_ids"]) \
                and not any(t.get("tool_ids") for t in cc.get("tools", [])):
            return rep.add("agent", agent["id"], "same")
        if not apply:
            return rep.add("agent", agent["id"], "update")
        st, r = c.kb("PUT", f"/api/agent_builder/agents/{agent['id']}", body)
    else:
        if not apply:
            return rep.add("agent", agent["id"], "create")
        st, r = c.kb("POST", "/api/agent_builder/agents", agent)
    rep.add("agent", agent["id"], "OK" if st == 200 else "ERROR", "" if st == 200 else json.dumps(r)[:300])


def install_rule(c, rep, apply, path, wf_ids):
    rule = json.loads(resolve(path.read_text(), wf_ids, rep, path.name))
    q = urllib.parse.quote(f'"{rule["name"]}"')
    st, found = c.kb("GET", f"/api/alerting/rules/_find?search_fields=name&search={q}&per_page=10")
    match = [r for r in found.get("data", []) if r["name"] == rule["name"]] if st == 200 else []
    body = {k: rule[k] for k in ("name", "tags", "schedule", "params", "actions")}
    if match:
        cur = match[0]
        cur_actions = [{"id": a["id"], "params": a["params"]} for a in cur.get("actions", [])]
        if norm(cur["params"]) == norm(rule["params"]) and norm(cur_actions) == norm(rule["actions"]) \
                and cur["schedule"] == rule["schedule"]:
            return rep.add("rule", rule["name"], "same", cur["id"])
        if not apply:
            return rep.add("rule", rule["name"], "update", cur["id"])
        st, r = c.kb("PUT", f"/api/alerting/rule/{cur['id']}", body)
    else:
        if not apply:
            return rep.add("rule", rule["name"], "create")
        st, r = c.kb("POST", "/api/alerting/rule", rule)
    rep.add("rule", rule["name"], "OK" if st == 200 else "ERROR", r.get("id", "") if st == 200 else json.dumps(r)[:300])


def patch_bundle_tool(c, rep, apply):
    """Elastic bundle bug: ad_ts_model_memory_health declares 'limit' as string but uses it in LIMIT."""
    st, t = c.kb("GET", "/api/agent_builder/tools/ad_ts_model_memory_health")
    if st != 200:
        return
    p = t.get("configuration", {}).get("params", {}).get("limit")
    if not p or p.get("type") == "integer":
        return rep.add("patch", "ad_ts_model_memory_health.limit", "same")
    if not apply:
        return rep.add("patch", "ad_ts_model_memory_health.limit", "update", "string -> integer")
    p["type"] = "integer"
    st, r = c.kb("PUT", "/api/agent_builder/tools/ad_ts_model_memory_health",
                 {k: t[k] for k in ("description", "configuration", "tags")})
    rep.add("patch", "ad_ts_model_memory_health.limit", "OK" if st == 200 else "ERROR")


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--components", default="ml,ingest")
    ap.add_argument("--book", choices=["auto", "yes", "no"], default="auto")
    ap.add_argument("--no-rules", action="store_true")
    a = ap.parse_args()
    apply = not (a.dry_run or a.check)
    comps = set(a.components.split(","))
    c = Client(load_env())
    rep = Report("apply" if apply else ("check" if a.check else "dry-run"))

    st, s = c.kb("GET", "/api/status")
    if st != 200:
        sys.exit(f"Cannot reach Kibana ({st}): {s}")
    print(f"Kibana {s.get('version', {}).get('number', '?')} - mode: {rep.mode}\n")

    wf_ids = list_workflows(c)
    if "ml" in comps:
        print("Prerequisites")
        for t in BUNDLE_TOOLS:
            st, _ = c.kb("GET", f"/api/agent_builder/tools/{t}")
            rep.add("tool", t, "present" if st == 200 else "MISSING-PREREQ",
                    "" if st == 200 else "register Elastic's kibana-anomaly-detection bundle first (see README)")
        st, _ = c.es("HEAD", f"/{BOOK_INDEX}")
        with_book = a.book == "yes" or (a.book == "auto" and st == 200)
        rep.add("index", BOOK_INDEX, "present" if st == 200 else "absent",
                "book tool/skill " + ("included" if with_book else "skipped"))
        if rep.errors and apply:
            sys.exit("\nMissing prerequisites - nothing changed.")

        print("\nIndex")
        install_index(c, rep, apply)
        print("\nWorkflows")
        for f in ML_WORKFLOWS:
            install_workflow(c, rep, apply, ROOT / "ml-remediation/workflows" / f, wf_ids)
        print("\nTools")
        patch_bundle_tool(c, rep, apply)
        for f in sorted((ROOT / "ml-remediation/tools").glob("*.json")):
            tool = json.loads(resolve(f.read_text(), wf_ids, rep, f.name))
            if tool["id"] == BOOK_TOOL and not with_book:
                continue
            install_tool(c, rep, apply, tool)
        print("\nSkills")
        skill_ids = []
        for f in sorted((ROOT / "ml-remediation/skills").glob("*.json")):
            skill = json.loads(f.read_text())
            if skill["id"] == BOOK_SKILL and not with_book:
                continue
            skill_ids.append(skill["id"])
            install_skill(c, rep, apply, skill)
        print("\nAgent")
        agent = json.loads((ROOT / "ml-remediation/agent.json").read_text())
        agent["configuration"]["skill_ids"] = [s for s in agent["configuration"]["skill_ids"] if s in skill_ids]
        if not with_book:
            agent["configuration"]["instructions"] = "\n".join(
                l for l in agent["configuration"]["instructions"].split("\n") if BOOK_SKILL not in l)
        install_agent(c, rep, apply, agent)
        if not a.no_rules:
            print("\nRules")
            for f in sorted((ROOT / "ml-remediation/rules").glob("*.json")):
                install_rule(c, rep, apply, f, wf_ids)

    if "ingest" in comps:
        print("\nIngest hardening")
        install_workflow(c, rep, apply, ROOT / "ingest-hardening/workflows/logstash-queue-backlog.yaml", wf_ids)
        st, _ = c.kb("GET", "/api/agent_builder/agents/ingest-steward")
        if st != 200:
            rep.add("agent", "ingest-steward", "note", "not found - the backlog workflow's diagnosis step will be skipped "
                    "(edit agent-id in logstash-queue-backlog.yaml to use another agent)")
        if not a.no_rules:
            install_rule(c, rep, apply, ROOT / "ingest-hardening/rules/logstash-queue-backlog.json", wf_ids)

    counts = {}
    for _, _, s, _ in rep.rows:
        counts[s] = counts.get(s, 0) + 1
    print("\nSummary:", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    sys.exit(1 if rep.errors else 0)


if __name__ == "__main__":
    import urllib.parse  # noqa: E402  (used by install_rule)
    main()
