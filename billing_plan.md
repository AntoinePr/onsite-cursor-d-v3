# Usage & Billing PLC — Roadmap

---

## Goal

Track agent LLM usage and costs in real time. Every LLM call produces a usage event that flows through a pipeline (Agent → Usage API → Redis → Cost Backend → Billing DB) and is pushed to the browser via WebSocket as it happens.

---

## Architecture

```mermaid
graph LR
  subgraph agentBox [Agent]
    Agent["Agent (LLM caller)"]
  end

  subgraph ingestion [Ingestion]
    UsageAPI["Usage API"]
  end

  subgraph queue [Queue]
    Redis["Redis"]
  end

  subgraph processing [Cost Processing]
    CostBackend["Cost Backend"]
    BillingDB["Billing DB - ephemeral"]
    CostBackend --> BillingDB
  end

  subgraph browser [Browser - Usage and Cost Tab]
    CostUI["Usage & Cost Panel"]
  end

  Agent -->|"POST /usage"| UsageAPI
  UsageAPI -->|"LPUSH"| Redis
  Redis -->|"BRPOP"| CostBackend
  CostBackend -->|"WebSocket"| CostUI

  style agentBox fill:#2d3748,stroke:#4a90d9,color:#fff
  style Agent fill:#4a90d9,stroke:#2b6cb0,color:#fff
  style ingestion fill:#2d3748,stroke:#ed8936,color:#fff
  style UsageAPI fill:#ed8936,stroke:#c05621,color:#fff
  style queue fill:#2d3748,stroke:#e53e3e,color:#fff
  style Redis fill:#e53e3e,stroke:#c53030,color:#fff
  style processing fill:#2d3748,stroke:#9f7aea,color:#fff
  style CostBackend fill:#9f7aea,stroke:#6b46c1,color:#fff
  style BillingDB fill:#9f7aea,stroke:#6b46c1,color:#fff
  style browser fill:#2d3748,stroke:#48bb78,color:#fff
  style CostUI fill:#48bb78,stroke:#2f855a,color:#fff
```

---

## Key Design Decisions

- **Agent as sole producer** — the control plane's LLM loop emits usage events after each completion; no other component writes usage.
- **Ephemeral Billing DB** — no Docker volume; all cost data is wiped on restart. This is intentional for the POC.
- **Decoupled ingestion and processing** — Usage API and Cost Backend are separate services connected by Redis, mimicking a production pipeline (Kafka, SQS, etc.).
- **Real-time delivery via WebSocket** — the Cost Backend pushes cost updates to the browser for sub-second latency.
- **Dedicated UI tab** — a new "Usage & Cost" tab in the browser's left panel, alongside the existing sessions list.
- **Arbitrary tags on usage events** — usage events carry a flexible JSONB `tags` field, enabling group-by breakdowns on any dimension without schema changes.

---

## Milestone 1: Minimal End-to-End Pipeline (~90 min)

**Objective**: Wire every component together and prove the full pipeline works. Every box in the architecture diagram exists, accepts input, and produces output. Intentionally bare-bones — no breakdowns, no limits, no rich UI.

### Key Outcomes

1. **All new infrastructure starts cleanly** — Redis, Billing DB (ephemeral), Usage API, and Cost Backend all come up with `make up`.
2. **Agent emits usage** — after each LLM streaming call completes, the control plane fires a usage event (model, token counts, session ID) to the Usage API.
3. **Usage flows through the pipeline** — Usage API enqueues to Redis; Cost Backend dequeues, computes cost from a hardcoded pricing table, and stores in Billing DB.
4. **Browser shows live cost** — a new "Usage & Cost" tab in the left panel connects via WebSocket and displays a running total (tokens + dollars) that updates in real time.
5. **Ephemeral data confirmed** — restarting the billing-db container wipes all cost data.

### Decisions for M1

- Usage event schema is minimal: `session_id`, `model`, `prompt_tokens`, `completion_tokens`, `total_tokens`, `timestamp`, `tags` (empty object for now).
- Cost computation uses a static in-memory pricing table (hardcoded per-model rates).
- The agent sends usage fire-and-forget (non-blocking); billing failures do not affect the chat flow.
- The "Usage & Cost" tab shows only a single cumulative counter — no per-call breakdown, no charts.

---

## Milestone 2: Usage & Cost Breakdowns (~90 min)

**Objective**: Let users explore their usage and costs with flexible grouping and a toggle between the two views.

### Key Outcomes

1. **Usage / Cost toggle** — the "Usage & Cost" tab lets users switch between viewing raw usage quantities and computed dollar costs in the graph.
2. **Group-by dimension selector** — users can group data by any combination of dimensions: `provider`, `model`, `usage_type`, `session_id`.
3. **Breakdown API** — a `GET /costs/breakdown?group_by=<dim>[,<dim>...]&metric=usage|cost` endpoint returns aggregated data for the selected dimensions and metric.
4. **Enriched UI** — the graph updates live as new events arrive, with the selected grouping and metric applied in real time.

### Decisions for M2

- Grouping is over first-class columns (`provider`, `model`, `usage_type`, `session_id`) — no JSONB tag queries yet.
- The `metric` toggle controls whether the API sums `quantity` (usage) or `total_cost` (cost).
- Multi-dimension grouping is supported (e.g. `group_by=model,session_id`).

---

## Milestone 3: Spending Limits (~90 min)

**Objective**: Give admins the ability to enforce spending caps that block agent activity when exceeded.

### Key Outcomes

1. **Spending limits** — admins can create limits scoped to a session or globally, with a configurable max cost.
2. **Limit enforcement in the pipeline** — after each cost record is written, the Cost Backend checks limits. If a limit is exceeded, it calls back to the control plane to block the session. The agent's LLM loop checks the blocked set before every call.
3. **Browser notifications** — when a limit is hit, the browser receives a WebSocket alert and displays a block banner.

### Decisions for M3

- Spending limits live in the Billing DB (`spending_limits` table with scope, max_cost).
- Enforcement is eventual — the event that crosses a limit is the last one allowed; the block takes effect before the next LLM call.
- The control plane exposes an internal-only `POST /internal/session/{id}/block` endpoint; the Cost Backend is the only caller.

---

## Future

- **Arbitrary tag-based grouping** — users can group usage and costs by any number of custom tags attached to usage events, enabling fully flexible multi-dimensional analysis.
