# Remote Tool Execution POC

A control plane that orchestrates LLM tool calls on remote workers via outbound-only WebSocket connections.

## Architecture

```
Browser ──WebSocket──▶ Control Plane (FastAPI) ◀──WebSocket── Worker 1
                             │                  ◀──WebSocket── Worker 2
                             ▼                  ◀──WebSocket── Worker 3
                         PostgreSQL
```

Workers connect **outbound** to the control plane — no inbound traffic into the customer environment.

## Prerequisites

- Docker & Docker Compose
- An OpenAI API key

## Quick Start

```bash
# 1. Create .env with your API key
cp .env.example .env
# Edit .env and set OPENAI_API_KEY=sk-...

# 2. Build and start all services
make up

# 3. Open the UI
open http://localhost:8000
```

## Services

| Service        | Port  | Description                        |
|----------------|-------|------------------------------------|
| control-plane  | 8000  | FastAPI app, serves UI + WebSocket |
| postgres       | 5432  | State persistence                  |
| worker-1/2/3   | —     | Tool execution workers (no ports)  |

## Usage

Open **http://localhost:8000** and try:

- "Get system info on worker-1"
- "Run `ls /tmp` on worker-2"
- "Run `uname -a` on all workers"

The LLM will call the appropriate tool, the control plane dispatches it to a worker, and the result is streamed back.

## Makefile Targets

```bash
make build          # Build Docker images
make up             # Build + start all services
make down           # Stop all services
make logs           # Tail all logs
make logs-cp        # Tail control plane logs
make logs-workers   # Tail worker logs
make reset-db       # Wipe DB and restart
make clean          # Remove everything (volumes + images)
```

## Running E2E Tests

```bash
pip install playwright
playwright install chromium
python e2e_test.py
```
