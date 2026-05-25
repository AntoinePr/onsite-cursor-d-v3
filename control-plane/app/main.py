import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from openai import AsyncOpenAI
from sqlalchemy import select

from app.database import async_session, init_db
from app.models import Message, Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("control-plane")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "execute_shell",
            "description": "Execute a shell command on a remote worker machine and return stdout/stderr. Use this for any command-line operation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The shell command to execute",
                    },
                    "worker_name": {
                        "type": "string",
                        "description": "Optional: specific worker to run on (e.g. worker-1, worker-2, worker-3). If omitted, any available worker is used.",
                    },
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_system_info",
            "description": "Get system information (hostname, OS, uptime, etc.) from a remote worker machine.",
            "parameters": {
                "type": "object",
                "properties": {
                    "worker_name": {
                        "type": "string",
                        "description": "Optional: specific worker to query. If omitted, any available worker is used.",
                    }
                },
                "required": [],
            },
        },
    },
]


# ── Worker Pool ──────────────────────────────────────────────────────────────


class WorkerPool:
    def __init__(self):
        self.workers: dict[str, WebSocket] = {}  # name -> websocket
        self._pending: dict[str, asyncio.Future] = {}  # tool_call_id -> future
        self._round_robin_idx = 0

    def register(self, name: str, ws: WebSocket):
        self.workers[name] = ws
        logger.info(f"Worker registered: {name} (total: {len(self.workers)})")

    def unregister(self, name: str):
        self.workers.pop(name, None)
        logger.info(f"Worker unregistered: {name} (total: {len(self.workers)})")

    def pick_worker(self, preferred: str | None = None) -> tuple[str, WebSocket] | None:
        if preferred and preferred in self.workers:
            return preferred, self.workers[preferred]
        if not self.workers:
            return None
        names = list(self.workers.keys())
        self._round_robin_idx = self._round_robin_idx % len(names)
        name = names[self._round_robin_idx]
        self._round_robin_idx += 1
        return name, self.workers[name]

    async def dispatch_tool_call(
        self, tool_call_id: str, function_name: str, arguments: dict
    ) -> str:
        preferred = arguments.pop("worker_name", None)
        target = self.pick_worker(preferred)
        if target is None:
            return json.dumps({"error": "No workers available"})

        worker_name, ws = target
        logger.info(f"Dispatching {function_name} (id={tool_call_id}) to {worker_name}")

        future: asyncio.Future[str] = asyncio.get_event_loop().create_future()
        self._pending[tool_call_id] = future

        await ws.send_json(
            {
                "type": "tool_call_request",
                "payload": {
                    "tool_call_id": tool_call_id,
                    "function_name": function_name,
                    "arguments": arguments,
                },
            }
        )

        try:
            result = await asyncio.wait_for(future, timeout=30.0)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(tool_call_id, None)
            return json.dumps({"error": f"Tool call timed out on {worker_name}"})

    def resolve_tool_call(self, tool_call_id: str, result: str):
        future = self._pending.pop(tool_call_id, None)
        if future and not future.done():
            future.set_result(result)

    def list_workers(self) -> list[str]:
        return list(self.workers.keys())


worker_pool = WorkerPool()

# ── Browser connections ──────────────────────────────────────────────────────

browser_connections: dict[str, list[WebSocket]] = {}  # session_id -> [websockets]


# ── App Lifecycle ────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("Database initialized")
    yield


app = FastAPI(title="Remote Tool Execution Control Plane", lifespan=lifespan)


# ── Routes ───────────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    ui_path = Path(__file__).parent / "ui" / "index.html"
    return HTMLResponse(ui_path.read_text())


@app.get("/api/workers")
async def list_workers():
    return {"workers": worker_pool.list_workers()}


@app.get("/api/sessions")
async def list_sessions():
    async with async_session() as db:
        result = await db.execute(
            select(Session).order_by(Session.created_at.desc())
        )
        sessions = result.scalars().all()
        return {
            "sessions": [
                {"id": str(s.id), "created_at": s.created_at.isoformat()}
                for s in sessions
            ]
        }


@app.get("/api/sessions/{session_id}/messages")
async def get_messages(session_id: str):
    async with async_session() as db:
        result = await db.execute(
            select(Message)
            .where(Message.session_id == uuid.UUID(session_id))
            .order_by(Message.created_at)
        )
        messages = result.scalars().all()
        return {
            "messages": [
                {
                    "id": str(m.id),
                    "role": m.role,
                    "content": m.content,
                    "tool_calls": m.tool_calls,
                    "tool_call_id": m.tool_call_id,
                }
                for m in messages
            ]
        }


# ── WebSocket: Browser Chat ─────────────────────────────────────────────────


@app.websocket("/ws/chat/{session_id}")
async def ws_chat(websocket: WebSocket, session_id: str):
    await websocket.accept()

    try:
        sid = uuid.UUID(session_id)
    except ValueError:
        sid = uuid.uuid5(uuid.NAMESPACE_DNS, session_id)

    async with async_session() as db:
        existing = await db.get(Session, sid)
        if not existing:
            session = Session(id=sid)
            db.add(session)
            await db.commit()

    session_id = str(sid)
    browser_connections.setdefault(session_id, []).append(websocket)
    logger.info(f"Browser connected to session {session_id}")

    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "user_message":
                try:
                    await handle_user_message(websocket, session_id, data["content"])
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
                    await websocket.send_json(
                        {"type": "error", "content": str(e)}
                    )
    except WebSocketDisconnect:
        conns = browser_connections.get(session_id, [])
        if websocket in conns:
            conns.remove(websocket)
        logger.info(f"Browser disconnected from session {session_id}")


async def handle_user_message(ws: WebSocket, session_id: str, content: str):
    async with async_session() as db:
        user_msg = Message(
            session_id=uuid.UUID(session_id), role="user", content=content
        )
        db.add(user_msg)
        await db.commit()

    await ws.send_json({"type": "status", "content": "Thinking..."})

    async with async_session() as db:
        result = await db.execute(
            select(Message)
            .where(Message.session_id == uuid.UUID(session_id))
            .order_by(Message.created_at)
        )
        history = result.scalars().all()

    messages = []
    for m in history:
        msg: dict = {"role": m.role}
        if m.role == "tool":
            msg["content"] = m.content or ""
            msg["tool_call_id"] = m.tool_call_id
        elif m.role == "assistant" and m.tool_calls:
            msg["content"] = m.content or ""
            msg["tool_calls"] = m.tool_calls
        else:
            msg["content"] = m.content or ""
        messages.append(msg)

    connected_workers = worker_pool.list_workers()
    system_msg = (
        "You are a helpful assistant with access to remote worker machines. "
        f"Currently connected workers: {connected_workers}. "
        "Use the provided tools to execute commands or get info from workers. "
        "When the user asks to run something on a specific worker, pass the worker_name parameter."
    )
    messages.insert(0, {"role": "system", "content": system_msg})

    await run_llm_loop(ws, session_id, messages)


async def run_llm_loop(ws: WebSocket, session_id: str, messages: list[dict]):
    """Call LLM, handle tool calls in a loop until we get a final text response."""
    for _ in range(10):  # max iterations to prevent infinite loops
        try:
            response = await openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=TOOLS_SCHEMA,
                tool_choice="auto",
            )
        except Exception as e:
            logger.error(f"LLM error: {e}")
            await ws.send_json({"type": "error", "content": f"LLM error: {e}"})
            return

        choice = response.choices[0]
        assistant_msg = choice.message

        tool_calls_data = None
        if assistant_msg.tool_calls:
            tool_calls_data = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in assistant_msg.tool_calls
            ]

        async with async_session() as db:
            db_msg = Message(
                session_id=uuid.UUID(session_id),
                role="assistant",
                content=assistant_msg.content,
                tool_calls=tool_calls_data,
            )
            db.add(db_msg)
            await db.commit()

        if assistant_msg.content:
            await ws.send_json(
                {"type": "assistant_message", "content": assistant_msg.content}
            )

        if choice.finish_reason == "stop" or not assistant_msg.tool_calls:
            return

        messages.append(
            {
                "role": "assistant",
                "content": assistant_msg.content or "",
                "tool_calls": tool_calls_data,
            }
        )

        for tc in assistant_msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                fn_args = {}

            await ws.send_json(
                {
                    "type": "tool_call_start",
                    "tool_call_id": tc.id,
                    "function_name": fn_name,
                    "arguments": fn_args,
                }
            )

            result = await worker_pool.dispatch_tool_call(tc.id, fn_name, fn_args)

            async with async_session() as db:
                tool_msg = Message(
                    session_id=uuid.UUID(session_id),
                    role="tool",
                    content=result,
                    tool_call_id=tc.id,
                )
                db.add(tool_msg)
                await db.commit()

            await ws.send_json(
                {
                    "type": "tool_call_result",
                    "tool_call_id": tc.id,
                    "function_name": fn_name,
                    "result": result,
                }
            )

            messages.append(
                {"role": "tool", "content": result, "tool_call_id": tc.id}
            )


# ── WebSocket: Worker ────────────────────────────────────────────────────────


@app.websocket("/ws/worker")
async def ws_worker(websocket: WebSocket):
    await websocket.accept()
    worker_name = None

    try:
        reg_data = await asyncio.wait_for(websocket.receive_json(), timeout=10.0)
        if reg_data.get("type") != "register":
            await websocket.close(code=4000, reason="Expected registration message")
            return

        worker_name = reg_data["payload"]["worker_name"]
        worker_pool.register(worker_name, websocket)

        await websocket.send_json(
            {"type": "registered", "payload": {"worker_name": worker_name}}
        )

        while True:
            data = await websocket.receive_json()
            if data.get("type") == "tool_call_result":
                payload = data["payload"]
                tool_call_id = payload["tool_call_id"]
                result = payload.get("result", "")
                error = payload.get("error")
                if error:
                    result = json.dumps({"error": error})
                worker_pool.resolve_tool_call(tool_call_id, result)
                logger.info(f"Tool result from {worker_name}: {tool_call_id}")
            elif data.get("type") == "heartbeat":
                await websocket.send_json({"type": "heartbeat_ack"})

    except WebSocketDisconnect:
        logger.info(f"Worker disconnected: {worker_name}")
    except asyncio.TimeoutError:
        logger.warning("Worker failed to register in time")
    finally:
        if worker_name:
            worker_pool.unregister(worker_name)
