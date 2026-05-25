import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from openai import AsyncOpenAI
from datetime import timedelta
from sqlalchemy import select, delete, and_

from app.database import async_session, init_db
from app.models import Message, Session, ToolCallDispatch, SESSION_TTL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("control-plane")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "bash",
            "description": "Run a bash command on a remote worker machine and return stdout/stderr.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The bash command to execute",
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
            "name": "read_file",
            "description": "Read the contents of a file on a remote worker machine.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute or relative path to the file to read",
                    },
                    "worker_name": {
                        "type": "string",
                        "description": "Optional: specific worker to read from.",
                    },
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Write content to a file on a remote worker machine. Creates parent directories if needed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute or relative path to the file to write",
                    },
                    "content": {
                        "type": "string",
                        "description": "The content to write to the file",
                    },
                    "worker_name": {
                        "type": "string",
                        "description": "Optional: specific worker to write to.",
                    },
                },
                "required": ["path", "content"],
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
        self._worker_session: dict[str, str] = {}   # worker_name -> session_id
        self._session_worker: dict[str, str] = {}   # session_id -> worker_name
        self._last_heartbeat: dict[str, datetime] = {}
        self._round_robin_idx = 0

    def register(self, name: str, ws: WebSocket, caps: list[str] | None = None):
        self.workers[name] = ws
        self.capabilities[name] = caps or []
        self._last_heartbeat[name] = datetime.now(timezone.utc)
        logger.info(f"Worker registered: {name} caps={caps} (total: {len(self.workers)})")

    def unregister(self, name: str):
        self.workers.pop(name, None)
        self.capabilities.pop(name, None)
        self._last_heartbeat.pop(name, None)
        sid = self._worker_session.pop(name, None)
        if sid:
            self._session_worker.pop(sid, None)
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

    async def bind_worker(self, worker_name: str, session_id: str):
        self._worker_session[worker_name] = session_id
        self._session_worker[session_id] = worker_name
        async with async_session() as db:
            sess = await db.get(Session, uuid.UUID(session_id))
            if sess:
                sess.bound_worker = worker_name
                await db.commit()
        logger.info(f"Bound {worker_name} to session {session_id[:8]}")

    async def unbind_worker(self, worker_name: str):
        sid = self._worker_session.pop(worker_name, None)
        if sid:
            self._session_worker.pop(sid, None)
            async with async_session() as db:
                sess = await db.get(Session, uuid.UUID(sid))
                if sess:
                    sess.bound_worker = None
                    await db.commit()
        logger.info(f"Unbound {worker_name}")

    def get_worker_for_session(self, session_id: str) -> str | None:
        return self._session_worker.get(session_id)

    async def bind_available_worker(self, session_id: str) -> str | None:
        """Eagerly bind an unbound worker to a session. Returns worker name or None."""
        already = self._session_worker.get(session_id)
        if already and already in self.workers:
            return already
        bound_names = set(self._worker_session.keys())
        eligible = [n for n in self.workers if n not in bound_names]
        if not eligible:
            return None
        name = eligible[0]
        await self.bind_worker(name, session_id)
        return name

    def pick_worker(
        self, function_name: str, session_id: str, preferred: str | None = None
    ) -> tuple[str, WebSocket] | None:
        already_bound = self._session_worker.get(session_id)
        if already_bound and already_bound in self.workers:
            return already_bound, self.workers[already_bound]

        bound_names = set(self._worker_session.keys())
        eligible = {
            n: ws for n, ws in self.workers.items()
            if n not in bound_names and function_name in self.capabilities.get(n, [])
        }
        if not eligible:
            eligible = {
                n: ws for n, ws in self.workers.items()
                if n not in bound_names
            }
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
        self, tool_call_id: str, function_name: str, arguments: dict,
        session_id: str = "",
    ) -> tuple[str, str]:
        """Returns (worker_name, result_json)."""
        preferred = arguments.pop("worker_name", None)
        target = self.pick_worker(function_name, session_id, preferred)
        if target is None:
            return "", json.dumps({"error": "No available workers. All workers are bound to sessions."})

        worker_name, ws = target
        if worker_name not in self._worker_session and session_id:
            await self.bind_worker(worker_name, session_id)

        message_id = str(uuid.uuid4())
        logger.info(f"Dispatching {function_name} (id={tool_call_id}, msg={message_id[:8]}) to {worker_name}")

        async with async_session() as db:
            db.add(ToolCallDispatch(
                tool_call_id=tool_call_id,
                session_id=uuid.UUID(session_id) if session_id else uuid.uuid4(),
                worker_name=worker_name,
                status="dispatched",
            ))
            await db.commit()

        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        self._pending[tool_call_id] = future
        self._pending_worker[tool_call_id] = worker_name

        await ws.send_json({
            "type": "tool_call_request",
            "message_id": message_id,
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
            async with async_session() as db:
                dispatch = await db.execute(
                    select(ToolCallDispatch).where(ToolCallDispatch.tool_call_id == tool_call_id)
                )
                row = dispatch.scalar_one_or_none()
                if row:
                    row.status = "failed"
                    await db.commit()
            return worker_name, json.dumps({"error": f"Tool call timed out on {worker_name}"})

    async def resolve_tool_call(self, tool_call_id: str, result: str):
        future = self._pending.pop(tool_call_id, None)
        self._pending_worker.pop(tool_call_id, None)
        if future and not future.done():
            future.set_result(result)
        async with async_session() as db:
            row = await db.execute(
                select(ToolCallDispatch).where(ToolCallDispatch.tool_call_id == tool_call_id)
            )
            dispatch = row.scalar_one_or_none()
            if dispatch:
                dispatch.status = "completed"
                dispatch.completed_at = datetime.now(timezone.utc)
                await db.commit()

    async def terminate_worker(self, worker_name: str, reason: str):
        ws = self.workers.get(worker_name)
        if ws:
            try:
                await ws.send_json({
                    "type": "terminate",
                    "payload": {"reason": reason},
                })
                logger.info(f"Sent terminate to {worker_name}: {reason}")
            except Exception:
                logger.warning(f"Failed to send terminate to {worker_name}")
        self.unregister(worker_name)

    async def terminate_worker_for_session(self, session_id: str, reason: str):
        worker_name = self._session_worker.get(session_id)
        if worker_name:
            await self.terminate_worker(worker_name, reason)

    def list_workers(self) -> list[dict]:
        return [
            {
                "name": n,
                "capabilities": self.capabilities.get(n, []),
                "session_id": self._worker_session.get(n),
            }
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
        if "message_id" not in msg:
            msg["message_id"] = str(uuid.uuid4())
        await self.outbound_queue.put(msg)

    async def _delivery_loop(self):
        while True:
            msg = await self.outbound_queue.get()
            while self.browser_ws is None:
                await asyncio.sleep(0.1)
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

    async with async_session() as db:
        result = await db.execute(
            select(ToolCallDispatch).where(
                ToolCallDispatch.status.in_(["dispatched", "acked"])
            )
        )
        stale = result.scalars().all()
        for dispatch in stale:
            dispatch.status = "failed"
        if stale:
            await db.commit()
            logger.info(f"Startup recovery: marked {len(stale)} in-flight dispatch(es) as failed")

    reaper = asyncio.create_task(_session_reaper())
    dispatch_reaper = asyncio.create_task(_dispatch_reaper())
    health_checker = asyncio.create_task(_worker_health_checker())
    yield
    logger.info("Shutting down gracefully...")
    reaper.cancel()
    dispatch_reaper.cancel()
    health_checker.cancel()
    for state in session_manager.all_sessions():
        if state.active_llm_task and not state.active_llm_task.done():
            state.active_llm_task.cancel()
        if state.browser_ws:
            try:
                await state.browser_ws.close(code=1001, reason="Server shutting down")
            except Exception:
                pass
        state.cleanup()
    for name in list(worker_pool.workers.keys()):
        try:
            await worker_pool.terminate_worker(name, "control plane shutting down")
        except Exception:
            pass
    logger.info("Shutdown complete")


async def _session_reaper():
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        expired = [s for s in session_manager.all_sessions() if s.expires_at < now]
        for state in expired:
            logger.info(f"Expiring session {state.session_id}")
            await worker_pool.terminate_worker_for_session(
                state.session_id, "session expired"
            )
            state.cleanup()
            session_manager.remove(state.session_id)
            async with async_session() as db:
                sess = await db.get(Session, uuid.UUID(state.session_id))
                if sess:
                    sess.status = "expired"
                    sess.bound_worker = None
                    await db.commit()


HEARTBEAT_TIMEOUT = timedelta(seconds=45)


async def _worker_health_checker():
    while True:
        await asyncio.sleep(10)
        now = datetime.now(timezone.utc)
        stale = [
            (name, ws)
            for name, ws in list(worker_pool.workers.items())
            if now - worker_pool._last_heartbeat.get(name, now) > HEARTBEAT_TIMEOUT
        ]
        for name, ws in stale:
            logger.warning(f"Worker {name} missed heartbeats, forcing disconnect")
            sid = worker_pool._worker_session.get(name)
            if sid:
                state = session_manager.get(sid)
                if state:
                    await state.send_to_browser({
                        "type": "worker_failure",
                        "worker_name": name,
                        "reason": "Worker heartbeat timeout",
                    })
                async with async_session() as db:
                    db.add(Message(
                        session_id=uuid.UUID(sid),
                        role="system",
                        content=json.dumps({"type": "worker_failure", "worker_name": name, "reason": "Worker heartbeat timeout"}),
                    ))
                    await db.commit()
            try:
                await ws.close(code=4002, reason="Heartbeat timeout")
            except Exception:
                pass


ACK_TIMEOUT = timedelta(seconds=5)


async def _dispatch_reaper():
    """Timeout unacked dispatches every 5s and trigger retries."""
    while True:
        await asyncio.sleep(5)
        cutoff = datetime.now(timezone.utc) - ACK_TIMEOUT
        try:
            async with async_session() as db:
                result = await db.execute(
                    select(ToolCallDispatch).where(
                        and_(
                            ToolCallDispatch.status == "dispatched",
                            ToolCallDispatch.dispatched_at < cutoff,
                        )
                    )
                )
                stale = result.scalars().all()
                for dispatch in stale:
                    dispatch.retry_count += 1
                    dispatch.status = "failed"
                    await db.commit()
                    future = worker_pool._pending.get(dispatch.tool_call_id)
                    if future and not future.done():
                        future.set_result(json.dumps({
                            "error": f"No ACK from {dispatch.worker_name} within {ACK_TIMEOUT.seconds}s",
                            "_retry": True,
                        }))
                        worker_pool._pending.pop(dispatch.tool_call_id, None)
                        worker_pool._pending_worker.pop(dispatch.tool_call_id, None)
                    logger.info(
                        f"Dispatch reaper: timed out {dispatch.tool_call_id} on {dispatch.worker_name} "
                        f"(retry #{dispatch.retry_count})"
                    )
        except Exception as e:
            logger.error(f"Dispatch reaper error: {e}")


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
            has_worker = worker_pool.get_worker_for_session(sid) is not None
            status = s.status
            if s.status == "active" and not has_worker and session_manager.get(sid) is not None:
                status = "failed"
            out.append({
                "id": sid,
                "name": s.name,
                "status": status,
                "has_worker": has_worker,
                "created_at": s.created_at.isoformat(),
                "last_active_at": s.last_active_at.isoformat() if s.last_active_at else None,
            })
        return {"sessions": out}


@app.get("/api/capacity")
async def api_capacity():
    total = len(worker_pool.workers)
    bound = len(worker_pool._worker_session)
    return {"available": total - bound, "total": total}


@app.delete("/api/sessions/{session_id}")
async def api_delete_session(session_id: str):
    sid = uuid.UUID(session_id)
    await worker_pool.terminate_worker_for_session(session_id, "session killed")
    session_manager.remove(session_id)
    async with async_session() as db:
        sess = await db.get(Session, sid)
        if sess:
            sess.status = "past"
            sess.bound_worker = None
            await db.commit()
    return {"killed": session_id}


@app.post("/api/sessions/{session_id}/reconnect")
async def api_reconnect_session(session_id: str):
    worker_name = await worker_pool.bind_available_worker(session_id)
    if not worker_name:
        return JSONResponse(status_code=409, content={"error": "No workers available"})
    sid = uuid.UUID(session_id)
    async with async_session() as db:
        sess = await db.get(Session, sid)
        if sess:
            sess.status = "active"
            await db.commit()
    async with async_session() as db:
        db.add(Message(
            session_id=sid,
            role="system",
            content=json.dumps({"type": "worker_reconnected", "worker_name": worker_name}),
        ))
        await db.commit()
    state = session_manager.get(session_id)
    if state:
        await state.send_to_browser({
            "type": "worker_reconnected",
            "worker_name": worker_name,
        })
    return {"ok": True, "worker_name": worker_name}


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


@app.post("/debug/replay/{tool_call_id}")
async def debug_replay(tool_call_id: str):
    """Re-send a tool_call_request to test worker idempotency."""
    async with async_session() as db:
        result = await db.execute(
            select(ToolCallDispatch).where(ToolCallDispatch.tool_call_id == tool_call_id)
        )
        dispatch = result.scalar_one_or_none()
        if not dispatch:
            return {"error": "Dispatch not found"}

    worker_name = dispatch.worker_name
    ws = worker_pool.workers.get(worker_name)
    if not ws:
        return {"error": f"Worker {worker_name} not connected"}

    msg = await db.execute(
        select(Message).where(
            and_(Message.tool_call_id == tool_call_id, Message.role == "assistant")
        )
    )

    message_id = str(uuid.uuid4())
    loop = asyncio.get_running_loop()
    future: asyncio.Future[str] = loop.create_future()
    worker_pool._pending[tool_call_id] = future
    worker_pool._pending_worker[tool_call_id] = worker_name

    await ws.send_json({
        "type": "tool_call_request",
        "message_id": message_id,
        "payload": {
            "tool_call_id": tool_call_id,
            "function_name": "replay",
            "arguments": {},
        },
    })

    try:
        result_str = await asyncio.wait_for(future, timeout=10.0)
        return {"replayed": tool_call_id, "worker": worker_name, "result": result_str, "message_id": message_id}
    except asyncio.TimeoutError:
        worker_pool._pending.pop(tool_call_id, None)
        worker_pool._pending_worker.pop(tool_call_id, None)
        return {"error": "Replay timed out"}


@app.get("/debug/dispatches")
async def debug_list_dispatches():
    """List recent dispatches for debugging."""
    async with async_session() as db:
        result = await db.execute(
            select(ToolCallDispatch).order_by(ToolCallDispatch.dispatched_at.desc()).limit(20)
        )
        rows = result.scalars().all()
        return {
            "dispatches": [
                {
                    "tool_call_id": r.tool_call_id,
                    "worker_name": r.worker_name,
                    "status": r.status,
                    "retry_count": r.retry_count,
                    "dispatched_at": r.dispatched_at.isoformat() if r.dispatched_at else None,
                    "acked_at": r.acked_at.isoformat() if r.acked_at else None,
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                }
                for r in rows
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
            available = len(worker_pool.workers) - len(worker_pool._worker_session)
            if available <= 0:
                await websocket.send_json({
                    "type": "error",
                    "content": "No available workers. All workers are bound to sessions. Delete a session to free one up.",
                })
                await websocket.close(code=4001, reason="No available workers")
                return
            db.add(Session(id=sid, status="active", last_active_at=now))
            await db.commit()
            await worker_pool.bind_available_worker(session_id)
        else:
            existing.status = "active"
            existing.last_active_at = now
            existing.expires_at = now + SESSION_TTL
            await db.commit()
            if existing.bound_worker and existing.bound_worker in worker_pool.workers:
                if existing.bound_worker not in worker_pool._worker_session:
                    worker_pool._worker_session[existing.bound_worker] = session_id
                    worker_pool._session_worker[session_id] = existing.bound_worker
                    logger.info(f"Restored binding on browser reconnect: {existing.bound_worker} -> session {session_id[:8]}")

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
        logger.info(f"Browser disconnected from session {session_id}")


# ── LLM Loop (decoupled from WebSocket) ─────────────────────────────────────


async def _generate_session_name(state: SessionState, first_message: str):
    """Call the LLM to generate a 1-2 word session name from the first user message."""
    sid = uuid.UUID(state.session_id)
    try:
        resp = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Summarize this user request in 1-2 words as a short session title. Reply with ONLY the title, nothing else."},
                {"role": "user", "content": first_message},
            ],
            max_tokens=10,
        )
        name = resp.choices[0].message.content.strip()[:50]
        async with async_session() as db:
            sess = await db.get(Session, sid)
            if sess:
                sess.name = name
                await db.commit()
        await state.send_to_browser({"type": "session_name_update", "session_id": state.session_id, "name": name})
        logger.info(f"Session {state.session_id[:8]} named: {name}")
    except Exception as e:
        logger.warning(f"Failed to generate session name: {e}")


async def handle_user_message(state: SessionState, content: str):
    sid = uuid.UUID(state.session_id)
    try:
        is_first_message = False
        async with async_session() as db:
            count = await db.execute(
                select(Message).where(Message.session_id == sid).limit(1)
            )
            is_first_message = count.scalar_one_or_none() is None
            db.add(Message(session_id=sid, role="user", content=content))
            await db.commit()

        if is_first_message:
            asyncio.create_task(_generate_session_name(state, content))

        await state.send_to_browser({"type": "status", "content": "Thinking..."})

        async with async_session() as db:
            result = await db.execute(
                select(Message).where(Message.session_id == sid).order_by(Message.created_at)
            )
            history = result.scalars().all()

        messages = []
        for m in history:
            if m.role == "system":
                continue
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

        bound_worker = worker_pool.get_worker_for_session(state.session_id)
        if bound_worker:
            worker_desc = f"You are bound to {bound_worker}."
        else:
            worker_desc = "You will be assigned a dedicated worker on your first tool call."
        system_msg = (
            "You are a helpful assistant with access to a remote worker machine. "
            f"{worker_desc} "
            "Use the provided tools to execute commands or get system info. "
            "Do not pass the worker_name parameter -- it is handled automatically."
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
            stream = await openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=TOOLS_SCHEMA,
                tool_choice="auto",
                stream=True,
            )
        except Exception as e:
            logger.error(f"LLM error: {e}")
            await state.send_to_browser({"type": "error", "content": f"LLM error: {e}"})
            return

        content_parts: list[str] = []
        tool_call_chunks: dict[int, dict] = {}
        finish_reason = None

        async for chunk in stream:
            delta = chunk.choices[0].delta if chunk.choices else None
            if not delta:
                continue

            if chunk.choices[0].finish_reason:
                finish_reason = chunk.choices[0].finish_reason

            if delta.content:
                content_parts.append(delta.content)
                await state.send_to_browser({
                    "type": "assistant_token",
                    "content": delta.content,
                })

            if delta.tool_calls:
                for tc_delta in delta.tool_calls:
                    idx = tc_delta.index
                    if idx not in tool_call_chunks:
                        tool_call_chunks[idx] = {
                            "id": tc_delta.id or "",
                            "type": "function",
                            "function": {"name": "", "arguments": ""},
                        }
                    entry = tool_call_chunks[idx]
                    if tc_delta.id:
                        entry["id"] = tc_delta.id
                    if tc_delta.function:
                        if tc_delta.function.name:
                            entry["function"]["name"] += tc_delta.function.name
                        if tc_delta.function.arguments:
                            entry["function"]["arguments"] += tc_delta.function.arguments

        full_content = "".join(content_parts) or None
        tool_calls_data = None
        if tool_call_chunks:
            tool_calls_data = [tool_call_chunks[i] for i in sorted(tool_call_chunks)]

        await state.send_to_browser({"type": "assistant_message_done"})

        async with async_session() as db:
            db.add(Message(
                session_id=sid,
                role="assistant",
                content=full_content,
                tool_calls=tool_calls_data,
            ))
            await db.commit()

        if full_content and not tool_calls_data:
            pass

        if finish_reason == "stop" or not tool_calls_data:
            return

        messages.append({
            "role": "assistant",
            "content": full_content or "",
            "tool_calls": tool_calls_data,
        })

        for tc in tool_calls_data:
            tc_id = tc["id"]
            fn_name = tc["function"]["name"]
            try:
                fn_args = json.loads(tc["function"]["arguments"])
            except json.JSONDecodeError:
                fn_args = {}

            await state.send_to_browser({
                "type": "tool_call_start",
                "tool_call_id": tc_id,
                "function_name": fn_name,
                "arguments": fn_args,
                "status": "pending",
            })

            result = None
            dispatched_worker = ""
            for attempt in range(max_retries + 1):
                dispatched_worker, result = await worker_pool.dispatch_tool_call(
                    tc_id, fn_name, fn_args.copy(), session_id=state.session_id,
                )
                if dispatched_worker:
                    await state.send_to_browser({
                        "type": "tool_call_update",
                        "tool_call_id": tc_id,
                        "status": "running",
                        "worker_name": dispatched_worker,
                    })

                try:
                    parsed = json.loads(result)
                    if parsed.get("_retry") and attempt < max_retries:
                        await state.send_to_browser({
                            "type": "tool_call_update",
                            "tool_call_id": tc_id,
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
                    session_id=sid, role="tool", content=result, tool_call_id=tc_id
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
                "tool_call_id": tc_id,
                "function_name": fn_name,
                "result": result,
                "worker_name": dispatched_worker,
                "status": "failed" if is_error else "complete",
            })

            messages.append({"role": "tool", "content": result, "tool_call_id": tc_id})


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

        async with async_session() as db:
            result = await db.execute(
                select(Session).where(
                    and_(Session.status == "active", Session.bound_worker == worker_name)
                )
            )
            bound_session = result.scalar_one_or_none()
            if bound_session:
                sid = str(bound_session.id)
                worker_pool._worker_session[worker_name] = sid
                worker_pool._session_worker[sid] = worker_name
                logger.info(f"Restored binding: {worker_name} -> session {sid[:8]}")

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
                await worker_pool.resolve_tool_call(tool_call_id, result)
                logger.info(f"Tool result from {worker_name}: {tool_call_id}")
            elif data.get("type") == "ack":
                msg_id = data.get("message_id", "")
                tc_id = data.get("tool_call_id", "")
                logger.info(f"ACK from {worker_name}: message_id={msg_id[:8]} tool_call_id={tc_id}")
                if tc_id:
                    async with async_session() as db:
                        row = await db.execute(
                            select(ToolCallDispatch).where(ToolCallDispatch.tool_call_id == tc_id)
                        )
                        dispatch = row.scalar_one_or_none()
                        if dispatch and dispatch.status == "dispatched":
                            dispatch.status = "acked"
                            dispatch.acked_at = datetime.now(timezone.utc)
                            await db.commit()
            elif data.get("type") == "heartbeat":
                worker_pool._last_heartbeat[worker_name] = datetime.now(timezone.utc)
                await websocket.send_json({"type": "heartbeat_ack"})

    except WebSocketDisconnect:
        logger.info(f"Worker disconnected: {worker_name}")
    except asyncio.TimeoutError:
        logger.warning("Worker failed to register in time")
    finally:
        if worker_name:
            sid = worker_pool._worker_session.get(worker_name)
            if sid:
                state = session_manager.get(sid)
                if state:
                    await state.send_to_browser({
                        "type": "worker_failure",
                        "worker_name": worker_name,
                        "reason": "Worker disconnected",
                    })
                async with async_session() as db:
                    db.add(Message(
                        session_id=uuid.UUID(sid),
                        role="system",
                        content=json.dumps({"type": "worker_failure", "worker_name": worker_name, "reason": "Worker disconnected"}),
                    ))
                    await db.commit()
            worker_pool.unregister(worker_name)
