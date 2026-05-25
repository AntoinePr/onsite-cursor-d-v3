# Remote Tool Execution POC — Demo

---

## The Problem

How do we execute tools in a **customer's environment** when we can't open inbound connections to it?

---

## Architecture

```mermaid
graph TB
  subgraph corePlane [Control Plane - Our Environment]
    CP["FastAPI Control Plane"]
    DB["PostgreSQL"]
    CP --> DB
  end

  subgraph browser [Browser]
    UI["React/HTML UI"]
  end

  subgraph customerEnv [Customer Environment]
    W1["Worker 1"]
    W2["Worker 2"]
    W3["Worker 3"]
  end

  UI -->|"WebSocket /ws/chat"| CP
  W1 -->|"WebSocket /ws/worker (outbound)"| CP
  W2 -->|"WebSocket /ws/worker (outbound)"| CP
  W3 -->|"WebSocket /ws/worker (outbound)"| CP

  style corePlane fill:#2d3748,stroke:#4a90d9,color:#fff
  style browser fill:#2d3748,stroke:#48bb78,color:#fff
  style customerEnv fill:#2d3748,stroke:#ed8936,color:#fff
  style CP fill:#4a90d9,stroke:#2b6cb0,color:#fff
  style DB fill:#4a90d9,stroke:#2b6cb0,color:#fff
  style UI fill:#48bb78,stroke:#2f855a,color:#fff
  style W1 fill:#ed8936,stroke:#c05621,color:#fff
  style W2 fill:#ed8936,stroke:#c05621,color:#fff
  style W3 fill:#ed8936,stroke:#c05621,color:#fff
```

**Key constraint**: No inbound traffic to customer environment. Workers initiate all connections outbound to the control plane.

---

## Initialization Flow

```mermaid
sequenceDiagram
  participant B as Browser
  participant CP as ControlPlane
  participant DB as PostgreSQL
  participant W as Worker

  Note over W,CP: Worker startup (outbound connection)
  W->>CP: Connect WebSocket /ws/worker
  W->>CP: Register capabilities (tool list, metadata)
  CP->>DB: Upsert worker record (status=connected)
  CP->>W: ACK registration

  Note over B,CP: Browser startup
  B->>CP: Connect WebSocket /ws/chat/{session_id}
  CP->>DB: Get or create session
  CP->>DB: Load conversation history
  CP->>B: Send existing messages (if any)
  CP->>B: Send connected workers list
```

---

## Tool Call Flow

```mermaid
sequenceDiagram
  box rgb(72,187,120) Browser
    participant B as Browser
  end
  box rgb(74,144,217) Control Plane
    participant CP as ControlPlane
    participant DB as PostgreSQL
  end
  box rgb(237,137,54) Customer Env
    participant W as Worker
  end

  Note over B,W: Initialization already complete

  B->>CP: User message (WebSocket)
  CP->>DB: Store message
  CP->>CP: Call LLM (OpenAI)
  CP->>DB: Store assistant response + tool_calls
  CP->>B: Stream: "calling tool X..."
  CP->>W: Send tool_call request (via worker WS)
  W->>W: Execute tool locally
  W->>CP: Return tool result
  CP->>DB: Store tool result
  CP->>CP: Call LLM with tool result
  CP->>B: Stream final answer
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Control Plane | Python 3.12, FastAPI, asyncio, websockets, SQLAlchemy + asyncpg |
| Database | PostgreSQL 16 |
| Workers | Python 3.12, websockets client, subprocess |
| UI | Single-file HTML/JS (served by FastAPI, no build step) |
| Infra | Docker Compose (6 services) |

---

## DB Tables

```mermaid
erDiagram
  sessions {
    uuid id PK
    timestamp created_at
  }

  messages {
    uuid id PK
    uuid session_id FK
    text role "user / assistant / tool"
    text content
    jsonb tool_calls
    text tool_call_id
    timestamp created_at
  }

  workers {
    uuid id PK
    text name
    text status "connected / disconnected"
    jsonb capabilities
    timestamp last_seen
  }

  tool_call_dispatch {
    uuid dispatch_id PK
    text tool_call_id
    uuid worker_id FK
    text status "dispatched / acked / completed / timeout / failed"
    timestamp dispatched_at
    timestamp acked_at
    timestamp completed_at
    int retry_count
  }

  sessions ||--o{ messages : "has"
  workers ||--o{ tool_call_dispatch : "executes"
```

---

## Failure Modes

| Name | Description | Behavior |
|------|-------------|----------|
| Worker failure | A worker crashes or disconnects mid-execution of a tool call | |
| Control plane failure | The control plane process crashes or restarts | |
| Tool call loss | A tool call request is sent but never reaches the worker (network drop, WS disconnect before delivery) | |
| Duplicate tool call | The same tool call is dispatched more than once (e.g. retry fires before late ACK arrives) | |
