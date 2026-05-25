import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from openai import AsyncOpenAI
from sqlalchemy import select, delete

from app.database import async_session, init_db
from app.models import Message, Session, SESSION_TTL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("control-plane")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "execute_shell",
            "description": "Execute a shell command on a remote worker machine and return stdout/stderr.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The shell command to execute",
                    },
                    "worker_name": {
                        "type": "string",
                        "description": "Optional: specific worker to run on (e.g. worker-1). If omitted, any available worker is used.",
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
            "description": "Get system information (hostname, OS, uptime) from a remote worker machine.",
            "parameters": {
                "type": "object",
                "properties": {
                    "worker_name": {
                        "type": "string",
                        "description": "Optional: specific worker to query.",
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
        self.workers: dict[str, WebSocket] = {}
        self.capabilities: dict[str, list[str]] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._pending_worker: dict[str, str] = {}  # tool_call_id -> worker_name
        self._round_robin_idx = 0

    def register(self, name: str, ws: WebSocket, caps: list[str] | None = None):
        self.workers[name] = ws
        self.capabilities[name] = caps or []
        logger.info(f"Worker registered: {name} caps={caps} (total: {len(self.workers)})")

    def unregister(self, name: str):
        self.workers.pop(name, None)
        self.capabilities.pop(name, None)
        failed_ids = [
            tid for tid, wn in self._pending_worker.items() if wn == name
        ]
        for tid in failed_ids:
            future = self._pending.pop(tid, None)
            self._pending_worker.pop(tid, None)
            if future and not future.done():
                future.set_result(json.dumps(
                    {"error": f"Worker {name} disconnected during execution", "_retry": True}
                ))
        logger.info(f"Worker unregistered: {name} (total: {len(self.workers)})")

    def pick_worker(
        self, function_name: str, preferred: str | None = None
    ) -> tuple[str, WebSocket] | None:
        eligible = {
            n: ws for n, ws in self.workers.items()
            if function_name in self.capabilities.get(n, [])
        }
        if not eligible:
            eligible = self.workers
        if not eligible:
            return None
        if preferred and preferred in eligible:
            return preferred, eligible[preferred]
        names = list(eligible.keys())
        self._round_robin_idx = self._round_robin_idx % len(names)
        name = names[self._round_robin_idx]
        self._round_robin_idx += 1
        return name, eligible[name]

    async def dispatch_tool_call(
        self, tool_call_id: str, function_name: str, arguments: dict
    ) -> tuple[str, str]:
        """Returns (worker_name, result_json)."""
        preferred = arguments.pop("worker_name", None)
        target = self.pick_worker(function_name, preferred)
        if target is None:
            return "", json.dumps({"error": "No workers available"})

        worker_name, ws = target
        logger.info(f"Dispatching {function_name} (id={tool_call_id}) to {worker_name}")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        self._pending[tool_call_id] = future
        self._pending_worker[tool_call_id] = worker_name

        await ws.send_json({
            "type": "tool_call_request",
            "payload": {
                "tool_call_id": tool_call_id,
                "function_name": function_name,
                "arguments": arguments,
            },
        })

        try:
            result = await asyncio.wait_for(future, timeout=30.0)
            return worker_name, result
        except asyncio.TimeoutError:
            self._pending.pop(tool_call_id, None)
            self._pending_worker.pop(tool_call_id, None)
            return worker_name, json.dumps({"error": f"Tool call timed out on {worker_name}"})

    def resolve_tool_call(self, tool_call_id: str, result: str):
        future = self._pending.pop(tool_call_id, None)
        self._pending_worker.pop(tool_call_id, None)
        if future and not future.done():
            future.set_result(result)

    def busy_worker_names(self) -> set[str]:
        return set(self._pending_worker.values())

    def list_workers(self) -> list[dict]:
        busy = self.busy_worker_names()
        return [
            {"name": n, "capabilities": self.capabilities.get(n, []), "busy": n in busy}
            for n in self.workers
        ]


worker_pool = WorkerPool()


# ── Session State & Manager ──────────────────────────────────────────────────


class SessionState:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.browser_ws: WebSocket | None = None
        self.outbound_queue: asyncio.Queue = asyncio.Queue()
        self.delivery_task: asyncio.Task | None = None
        self.active_llm_task: asyncio.Task | None = None
        self.last_active_at: datetime = datetime.now(timezone.utc)
        self.expires_at: datetime = self.last_active_at + SESSION_TTL

    def touch(self):
        self.last_active_at = datetime.now(timezone.utc)
        self.expires_at = self.last_active_at + SESSION_TTL

    async def send_to_browser(self, msg: dict):
        await self.outbound_queue.put(msg)

    async def _delivery_loop(self):
        while True:
            msg = await self.outbound_queue.get()
            if self.browser_ws is not None:
                try:
                    await self.browser_ws.send_json(msg)
                except Exception:
                    self.browser_ws = None

    def start_delivery(self):
        if self.delivery_task is None or self.delivery_task.done():
            self.delivery_task = asyncio.create_task(self._delivery_loop())

    def attach_browser(self, ws: WebSocket):
        self.browser_ws = ws
        self.touch()
        self.start_delivery()

    def detach_browser(self):
        self.browser_ws = None
        self.touch()

    def cleanup(self):
        if self.delivery_task and not self.delivery_task.done():
            self.delivery_task.cancel()
        if self.active_llm_task and not self.active_llm_task.done():
            self.active_llm_task.cancel()


class SessionManager:
    def __init__(self):
        self._sessions: dict[str, SessionState] = {}

    def get_or_create(self, session_id: str) -> SessionState:
        if session_id not in self._sessions:
            self._sessions[session_id] = SessionState(session_id)
        return self._sessions[session_id]

    def get(self, session_id: str) -> SessionState | None:
        return self._sessions.get(session_id)

    def remove(self, session_id: str):
        state = self._sessions.pop(session_id, None)
        if state:
            state.cleanup()

    def all_sessions(self) -> list[SessionState]:
        return list(self._sessions.values())


session_manager = SessionManager()


# ── App Lifecycle ────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("Database initialized")
    reaper = asyncio.create_task(_session_reaper())
    yield
    reaper.cancel()


async def _session_reaper():
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        expired = [s for s in session_manager.all_sessions() if s.expires_at < now]
        for state in expired:
            logger.info(f"Expiring session {state.session_id}")
            state.cleanup()
            session_manager.remove(state.session_id)
            async with async_session() as db:
                sess = await db.get(Session, uuid.UUID(state.session_id))
                if sess:
                    sess.status = "expired"
                    await db.commit()


app = FastAPI(title="Remote Tool Execution Control Plane", lifespan=lifespan)


# ── Routes ───────────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    ui_path = Path(__file__).parent / "ui" / "index.html"
    return HTMLResponse(ui_path.read_text())


@app.get("/api/workers")
async def api_list_workers():
    return {"workers": worker_pool.list_workers()}


@app.get("/api/sessions")
async def api_list_sessions():
    async with async_session() as db:
        result = await db.execute(
            select(Session).order_by(Session.created_at.desc())
        )
        sessions = result.scalars().all()
        out = []
        for s in sessions:
            sid = str(s.id)
            state = session_manager.get(sid)
            if state and state.active_llm_task and not state.active_llm_task.done():
                status = "active"
            elif s.status == "expired":
                status = "expired"
            elif state and state.browser_ws is not None:
                status = "connected"
            else:
                status = "idle"
            out.append({
                "id": sid,
                "status": status,
                "created_at": s.created_at.isoformat(),
                "last_active_at": s.last_active_at.isoformat() if s.last_active_at else None,
            })
        return {"sessions": out}


@app.delete("/api/sessions/{session_id}")
async def api_delete_session(session_id: str):
    sid = uuid.UUID(session_id)
    session_manager.remove(session_id)
    async with async_session() as db:
        await db.execute(delete(Message).where(Message.session_id == sid))
        await db.execute(delete(Session).where(Session.id == sid))
        await db.commit()
    return {"deleted": session_id}


@app.get("/api/sessions/{session_id}/messages")
async def api_get_messages(session_id: str):
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

    session_id = str(sid)
    now = datetime.now(timezone.utc)

    async with async_session() as db:
        existing = await db.get(Session, sid)
        if not existing:
            db.add(Session(id=sid, status="active", last_active_at=now))
            await db.commit()
        else:
            existing.status = "active"
            existing.last_active_at = now
            existing.expires_at = now + SESSION_TTL
            await db.commit()

    state = session_manager.get_or_create(session_id)
    state.attach_browser(websocket)
    is_reconnect = False

    # Replay history from DB
    async with async_session() as db:
        result = await db.execute(
            select(Message)
            .where(Message.session_id == sid)
            .order_by(Message.created_at)
        )
        history = result.scalars().all()
        if history:
            is_reconnect = True

    if is_reconnect:
        await websocket.send_json({
            "type": "history_replay",
            "messages": [
                {
                    "id": str(m.id),
                    "role": m.role,
                    "content": m.content,
                    "tool_calls": m.tool_calls,
                    "tool_call_id": m.tool_call_id,
                }
                for m in history
            ],
        })

    is_busy = state.active_llm_task is not None and not state.active_llm_task.done()
    await websocket.send_json({
        "type": "session_status",
        "session_id": session_id,
        "is_reconnect": is_reconnect,
        "is_busy": is_busy,
    })

    logger.info(f"Browser connected to session {session_id} (reconnect={is_reconnect})")

    try:
        while True:
            data = await websocket.receive_json()
            state.touch()
            async with async_session() as db:
                sess = await db.get(Session, sid)
                if sess:
                    sess.last_active_at = state.last_active_at
                    sess.expires_at = state.expires_at
                    await db.commit()

            if data.get("type") == "user_message":
                if state.active_llm_task and not state.active_llm_task.done():
                    await state.send_to_browser(
                        {"type": "error", "content": "A request is already in progress. Please wait."}
                    )
                    continue
                state.active_llm_task = asyncio.create_task(
                    handle_user_message(state, data["content"])
                )
    except WebSocketDisconnect:
        state.detach_browser()
        async with async_session() as db:
            sess = await db.get(Session, sid)
            if sess:
                sess.status = "disconnected"
                sess.last_active_at = state.last_active_at
                sess.expires_at = state.expires_at
                await db.commit()
        logger.info(f"Browser disconnected from session {session_id}")


# ── LLM Loop (decoupled from WebSocket) ─────────────────────────────────────


async def handle_user_message(state: SessionState, content: str):
    sid = uuid.UUID(state.session_id)
    try:
        async with async_session() as db:
            db.add(Message(session_id=sid, role="user", content=content))
            await db.commit()

        await state.send_to_browser({"type": "status", "content": "Thinking..."})

        async with async_session() as db:
            result = await db.execute(
                select(Message).where(Message.session_id == sid).order_by(Message.created_at)
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

        workers = worker_pool.list_workers()
        worker_desc = ", ".join(f"{w['name']} ({', '.join(w['capabilities'])})" for w in workers) or "none"
        system_msg = (
            "You are a helpful assistant with access to remote worker machines. "
            f"Connected workers: {worker_desc}. "
            "Use the provided tools to execute commands or get info from workers. "
            "When the user asks to run something on a specific worker, pass the worker_name parameter."
        )
        messages.insert(0, {"role": "system", "content": system_msg})

        await run_llm_loop(state, messages)
    except asyncio.CancelledError:
        logger.info(f"LLM task cancelled for session {state.session_id}")
    except Exception as e:
        logger.error(f"Error in LLM loop for {state.session_id}: {e}")
        await state.send_to_browser({"type": "error", "content": str(e)})


async def run_llm_loop(state: SessionState, messages: list[dict]):
    sid = uuid.UUID(state.session_id)
    max_retries = 2

    for _ in range(10):
        try:
            response = await openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=TOOLS_SCHEMA,
                tool_choice="auto",
            )
        except Exception as e:
            logger.error(f"LLM error: {e}")
            await state.send_to_browser({"type": "error", "content": f"LLM error: {e}"})
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
            db.add(Message(
                session_id=sid,
                role="assistant",
                content=assistant_msg.content,
                tool_calls=tool_calls_data,
            ))
            await db.commit()

        if assistant_msg.content:
            await state.send_to_browser(
                {"type": "assistant_message", "content": assistant_msg.content}
            )

        if choice.finish_reason == "stop" or not assistant_msg.tool_calls:
            return

        messages.append({
            "role": "assistant",
            "content": assistant_msg.content or "",
            "tool_calls": tool_calls_data,
        })

        for tc in assistant_msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                fn_args = {}

            await state.send_to_browser({
                "type": "tool_call_start",
                "tool_call_id": tc.id,
                "function_name": fn_name,
                "arguments": fn_args,
                "status": "pending",
            })

            result = None
            dispatched_worker = ""
            for attempt in range(max_retries + 1):
                dispatched_worker, result = await worker_pool.dispatch_tool_call(
                    tc.id, fn_name, fn_args.copy()
                )
                if dispatched_worker:
                    await state.send_to_browser({
                        "type": "tool_call_update",
                        "tool_call_id": tc.id,
                        "status": "running",
                        "worker_name": dispatched_worker,
                    })

                try:
                    parsed = json.loads(result)
                    if parsed.get("_retry") and attempt < max_retries:
                        await state.send_to_browser({
                            "type": "tool_call_update",
                            "tool_call_id": tc.id,
                            "status": "retrying",
                            "worker_name": dispatched_worker,
                            "error": parsed.get("error", "Worker disconnected"),
                        })
                        logger.info(
                            f"Retrying {fn_name} (attempt {attempt+2}) after worker {dispatched_worker} failed"
                        )
                        continue
                except (json.JSONDecodeError, AttributeError):
                    pass
                break

            async with async_session() as db:
                db.add(Message(
                    session_id=sid, role="tool", content=result, tool_call_id=tc.id
                ))
                await db.commit()

            is_error = False
            try:
                parsed = json.loads(result)
                is_error = "error" in parsed
            except (json.JSONDecodeError, TypeError):
                pass

            await state.send_to_browser({
                "type": "tool_call_result",
                "tool_call_id": tc.id,
                "function_name": fn_name,
                "result": result,
                "worker_name": dispatched_worker,
                "status": "failed" if is_error else "complete",
            })

            messages.append({"role": "tool", "content": result, "tool_call_id": tc.id})


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

        payload = reg_data["payload"]
        worker_name = payload["worker_name"]
        caps = payload.get("capabilities", [])
        worker_pool.register(worker_name, websocket, caps)

        await websocket.send_json(
            {"type": "registered", "payload": {"worker_name": worker_name}}
        )

        while True:
            data = await websocket.receive_json()
            if data.get("type") == "tool_call_result":
                p = data["payload"]
                tool_call_id = p["tool_call_id"]
                result = p.get("result", "")
                error = p.get("error")
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
