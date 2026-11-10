# Lessons learned (Elastic 9.5.6)

Behaviour we hit while building this kit. Each item cost at least one failed attempt.

## Kibana Workflows API
- **Routes:**
  - `GET/PUT /api/workflows/workflow/{id}` (body `{"yaml": ...}`)
  - `POST /api/workflows/workflow/{id}/run` (`{"inputs": {}}`)
  - `POST /api/workflows/test` (`{"workflowYaml": ...}`)
  - `GET /api/workflows/executions/{execId}`
  - `GET /api/workflows/schema?loose=false` (the full step-type schema)
- **`POST /api/workflows?overwrite=true` with an existing name creates a duplicate** (`<id>-1`); it doesn't overwrite. Use PUT to update. The installer only POSTs names that don't exist yet.
- Step outputs aren't in the execution API response. Read them from the `.workflows-step-executions` index (field `workflowRunId`).
- In Liquid, `execution.startedAt` and `now` render as JavaScript date strings, not ISO 8601, so `@timestamp` fails to parse. Let an ingest pipeline set it.
- `cases.createCase` returns the ID at `output.case.id`, not `output.id`.
- A step failure fails the whole run unless the step has `on-failure: continue: true`. Put it on non-critical steps (comments, agent calls) so logging still runs.
- `foreach` over `'${{ steps.x.output.csv | split: "," | compact }}'` works. `compact` does not drop empty strings, so guard with `if: 'not foreach.item: ""'`.

## Alerting → Workflows
- An ES|QL rule with `groupBy: row` passes each row's columns to the workflow at `event.alerts[].kibana.alert.grouping.<column>`.
- **Only keyword/text columns show up there.** `TO_STRING()` numeric columns after the threshold `WHERE`, and alias dotted names (`EVAL pipeline = logstash.pipeline.name`).
- Group the rule (`STATS ... BY job_id`) so you get one alert per entity, not one per matching document.
- Alerts that stay active are *ongoing*. If the action is set to `new` only, an ongoing problem triggers the workflow just once.

## Agent Builder
- **User-created skills can reference at most 5 tools** (`"A skill can reference at most 5 tools"`). Plan skills around job phases, not tool count.
- Agents load skills on demand (`load_skill`), so an agent with 0 direct tools and N skills works well.
- `confirmation.askUser: "always"` on a tool makes Kibana ask the user before running it. This enforces approval in the platform, not just in the prompt.
- `POST /api/agent_builder/agents` rejects a `type` field.
- ES|QL tool parameters are typed. A `string` parameter used in `LIMIT ?x` fails, and `?hours * 1 hour` fails (time arithmetic needs a literal), so use `DATE_DIFF("hour", ts, NOW()) < ?hours`.
- Workflow `ai.agent` steps return the reply at `output.message`. A full diagnosis costs about 50–60k tokens.

## Knowledge base
- An uploaded PDF becomes **one document**. Split it into per-page passages before exposing it as a search tool, or every lookup pulls the whole book into context.
- ES|QL hybrid search works: `FORK (WHERE MATCH(semantic_field, ?q) ...) (WHERE MATCH(text_field, ?q) ...) | FUSE`.

## Elasticsearch / ML
- A datafeed restart **never backfills**. Even with `start` set earlier, it resumes after the job's latest processed record.
- `query_delay` comes back in milliseconds (`74457ms`). Normalise it before doing arithmetic.
- `blocked.reason: revert` with a task that no longer exists (*"isn't running and hasn't stored its results"*) is a stale flag. Re-running `_revert` against the job's *current* `model_snapshot_id` with `delete_intervening_results: false` clears it without changing the model.
- Data streams created together under a time-based ILM rollover stay synchronized for good, and each period's warm migration follows in lockstep.
- Transient cluster settings are easy to forget and are lost on a full restart. Check them when recoveries look too aggressive.
