# Usage & Billing System — Demo

---

## The Problem

How do we **track and control LLM costs** in real time, across sessions and models, with flexible breakdowns and spending limits?

---

## Demo Agenda

1. **Architecture overview** — how usage events flow from agent to browser
2. **End-to-end pipeline** — emit a usage event, watch it appear as a live cost update
3. **Breakdowns & limits** — slice costs by any dimension; enforce spending caps

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
    BillingDB["Billing DB (ephemeral)"]
    CostBackend --> BillingDB
  end

  subgraph browser [Browser]
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

## Pipeline Flow

```mermaid
sequenceDiagram
  participant A as Agent
  participant U as Usage API
  participant R as Redis
  participant C as Cost Backend
  participant DB as Billing DB
  participant B as Browser

  A->>U: POST /usage (model, tokens, session_id, tags)
  U->>R: LPUSH usage_queue
  R->>C: BRPOP usage_queue
  C->>C: Compute cost (static pricing table)
  C->>DB: INSERT cost record
  C->>B: WebSocket push (live cost update)
```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Agent is the sole usage producer | Single source of truth — only the LLM loop emits events |
| Ephemeral Billing DB (no Docker volume) | POC trade-off — all cost data wipes on restart |
| Decoupled ingestion & processing | Usage API and Cost Backend are separate services joined by Redis, mirroring production patterns (Kafka, SQS) |
| Fire-and-forget from agent | Billing failures never block the chat flow |
| Flexible JSONB `tags` on events | Enables arbitrary group-by breakdowns without schema changes |
| Real-time WebSocket delivery | Sub-second cost updates pushed to the browser |

---

## Data Schema

Data flows through three successive shapes: the **ingestion event**, the **usage record**, and the **cost record**.

### 1. Usage Event (POST /usage → Redis)

| Field | Description |
|-------|-------------|
| `event_type` | Kind of event (e.g. `llm_completion`) |
| `org_id` | Organization identifier |
| `provider` | LLM provider (e.g. `openai`) |
| `model` | Model used (e.g. `gpt-4o`) |
| `session_id` | Session that triggered the call |
| `timestamp` | ISO-8601 event time |
| `usage` | Dict of usage quantities — keys are usage types (e.g. `prompt_tokens`, `completion_tokens`) |

### 2. `usage` table (Billing DB)

| Column | Description |
|--------|-------------|
| `id` | Primary key (UUID) |
| `event_id` | Unique event identifier for dedup |
| `org_id` | Organization identifier |
| `session_id` | Session that produced this usage |
| `provider` | LLM provider |
| `model` | Model name |
| `event_type` | Kind of event |
| `usage_type` | Specific metric (e.g. `prompt_tokens`) |
| `quantity` | Raw count for this usage type |
| `created_at` | Timestamp of record creation |

> One ingestion event fans out into multiple `usage` rows — one per key in the `usage` dict.

### 3. `costs` table (Billing DB)

| Column | Description |
|--------|-------------|
| `id` | Primary key (UUID) |
| `usage_id` | FK → `usage.id` (cascading delete) |
| `usage_type` | Mirrors usage type for easy querying |
| `unit_cost` | Per-unit price applied |
| `total_cost` | `quantity × unit_cost` |
| `created_at` | Timestamp of record creation |

---

## Tag-Based Breakdowns

- `GET /costs/breakdown?group_by=model` — cost per model
- `GET /costs/breakdown?group_by=session_id,model` — cost per session per model
- `GET /costs/breakdown?group_by=worker_name&filter_tag=model:gpt-4o` — cost per worker for a specific model

Powered by dynamic SQL aggregation over JSONB tags — no pre-aggregation tables needed.

---

## Spending Limits

| Scope | Action | Behavior |
|-------|--------|----------|
| Session | **Warn** | Push warning banner to browser when threshold is approached |
| Session | **Block** | Stop all LLM calls for the session; notify browser |
| Global | **Warn / Block** | Same actions, applied across all sessions |

Enforcement is **eventual** — the event that crosses a limit is the last one allowed; the block takes effect before the next LLM call.

```mermaid
sequenceDiagram
  participant C as Cost Backend
  participant DB as Billing DB
  participant CP as Control Plane
  participant B as Browser

  C->>DB: Write cost record
  C->>DB: Check spending limits
  alt Limit exceeded
    C->>CP: POST /internal/session/{id}/block
    CP->>B: WebSocket alert (blocked)
  end
```
