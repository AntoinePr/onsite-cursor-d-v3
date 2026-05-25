# Remote Tool Execution POC — Demo

---

## The Problem

How do we execute tools in a **customer's environment** when we can't open inbound connections to it?

---

## Demo Agenda

1. **Architecture presentation** — how the pieces fit together
2. **System demo** — end-to-end flow
   - Create a session, send a message, watch tool dispatch + streaming response
   - Show worker binding, session persistence, and reconnect
3. **Failure modes demo** — resilience under stress
   - Kill a worker mid-tool-call → retry on another worker
   - Browser disconnect → reconnect with full history replay
   - Duplicate tool call → idempotent execution (cached result)

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

  Note over CP,W: Worker startup (outbound connection)
  W->>CP: Connect WebSocket
  W->>CP: Register capabilities
  CP->>CP: Store worker in-memory pool
  CP->>W: ACK registration

  Note over B,CP: Browser startup
  B->>CP: GET workers and sessions
  CP->>B: Worker list + session list
  Note over B: User clicks New Session
  B->>CP: Connect WebSocket
  CP->>DB: Create session, bind worker
  CP->>B: Session status + history replay
```

---

## Tool Call Flow

```mermaid
sequenceDiagram
  participant B as Browser
  participant CP as ControlPlane
  participant DB as PostgreSQL
  participant W as Worker

  Note over B,W: Initialization already complete

  B->>CP: User message (WebSocket)
  CP->>DB: Store message
  CP->>CP: Call LLM (OpenAI, streaming)
  CP->>B: Stream tokens (assistant_token messages)
  CP->>DB: Store assistant response + tool_calls
  CP->>B: tool_call_start
  CP->>DB: Create tool_call_dispatch (status=dispatched)
  CP->>W: Send tool_call_request (message_id)
  W->>CP: ACK (message_id, tool_call_id)
  CP->>DB: Update dispatch (status=acked)
  W->>W: Execute tool locally
  W->>CP: Return tool_call_result
  CP->>DB: Update dispatch (status=completed)
  CP->>DB: Store tool result message
  CP->>B: tool_call_result
  CP->>CP: Call LLM with tool result (streaming)
  CP->>B: Stream final answer tokens
```

---

## DB Tables

```mermaid
erDiagram
  sessions {
    uuid id PK
    varchar name "auto-generated title"
    text status "active / past / expired / failed"
    timestamp created_at
    timestamp last_active_at
    timestamp expires_at
  }

  messages {
    uuid id PK
    uuid session_id FK
    text role "user / assistant / tool / system"
    text content
    jsonb tool_calls
    text tool_call_id
    timestamp created_at
  }

  tool_call_dispatch {
    uuid id PK
    text tool_call_id UK
    uuid session_id FK
    text worker_name
    text status "dispatched / acked / completed / failed"
    timestamp dispatched_at
    timestamp acked_at
    timestamp completed_at
    int retry_count
  }

  workers {
    uuid id PK
    text name
    text status "connected / disconnected"
    jsonb capabilities
    timestamp last_seen
  }

  sessions ||--o{ messages : "has"
  sessions ||--o{ tool_call_dispatch : "has"
```

> **Note**: The `workers` table exists in the schema but is not actively used at runtime. Worker state is managed in-memory via the `WorkerPool` class.

---

## System behaviors

| Name | Description | Behavior |
|------|-------------|----------|
| Worker failure | Worker crashes or disconnects mid-tool-call | Tool call is retried on another available worker (up to 2 retries) |
| Control plane failure | Control plane process crashes or restarts | Workers auto-reconnect; sessions persist in DB and replay on reconnect |
| Tool call loss | Tool call request sent but no ACK received | Dispatch reaper detects unacked dispatches after 5s timeout; marks as failed and triggers retry |
| Duplicate tool call | Same tool call dispatched more than once | Workers cache completed tool_call_ids (60s TTL) and return cached results without re-executing |
| User disconnects | Browser closes or loses connectivity | LLM loop keeps running; messages queue in-memory and drain on reconnect; browser auto-reconnects after 2s with full history replay |
| Session expiry | Session idle beyond TTL | Reaper task checks every 60s; expires sessions 1 hour after last activity; terminates bound worker and marks session as expired |
