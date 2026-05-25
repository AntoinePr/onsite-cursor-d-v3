import asyncio
import json
import logging
import os
import platform
import signal
import subprocess
import sys
import time

import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "ws://localhost:8000/ws/worker")
WORKER_NAME = os.getenv("WORKER_NAME", "worker-unknown")

CAPABILITIES = ["bash", "read_file", "write_file", "get_system_info"]


def bash(command: str) -> str:
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=25,
        )
        output = ""
        if result.stdout:
            output += result.stdout
        if result.stderr:
            output += f"\n[stderr] {result.stderr}"
        if result.returncode != 0:
            output += f"\n[exit code: {result.returncode}]"
        return output.strip() or "(no output)"
    except subprocess.TimeoutExpired:
        return "[error] Command timed out after 25 seconds"
    except Exception as e:
        return f"[error] {e}"


def read_file(path: str) -> str:
    try:
        with open(path, "r") as f:
            return f.read()
    except FileNotFoundError:
        return f"[error] File not found: {path}"
    except PermissionError:
        return f"[error] Permission denied: {path}"
    except Exception as e:
        return f"[error] {e}"


def write_file(path: str, content: str) -> str:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
        return f"OK: wrote {len(content)} bytes to {path}"
    except PermissionError:
        return f"[error] Permission denied: {path}"
    except Exception as e:
        return f"[error] {e}"


def get_system_info() -> str:
    info = {
        "hostname": platform.node(),
        "worker_name": WORKER_NAME,
        "os": platform.system(),
        "os_version": platform.version(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "uptime_note": "running in container",
    }
    return json.dumps(info, indent=2)


TOOL_HANDLERS = {
    "bash": lambda args: bash(args.get("command", "echo 'no command'")),
    "read_file": lambda args: read_file(args.get("path", "")),
    "write_file": lambda args: write_file(args.get("path", ""), args.get("content", "")),
    "get_system_info": lambda args: get_system_info(),
}


IDEMPOTENCY_TTL = 60
_completed_calls: dict[str, tuple[str, float]] = {}


def _evict_stale_cache():
    now = time.time()
    expired = [k for k, (_, ts) in _completed_calls.items() if now - ts > IDEMPOTENCY_TTL]
    for k in expired:
        del _completed_calls[k]


async def handle_tool_call(ws, payload: dict):
    tool_call_id = payload["tool_call_id"]
    function_name = payload["function_name"]
    arguments = payload.get("arguments", {})

    _evict_stale_cache()

    cached = _completed_calls.get(tool_call_id)
    if cached and (time.time() - cached[1]) < IDEMPOTENCY_TTL:
        logger.info(f"Idempotency cache hit for {tool_call_id}, returning cached result")
        result = cached[0]
    else:
        logger.info(f"Executing {function_name} (id={tool_call_id})")
        handler = TOOL_HANDLERS.get(function_name)
        if handler is None:
            result = json.dumps({"error": f"Unknown function: {function_name}"})
        else:
            result = await asyncio.get_event_loop().run_in_executor(None, handler, arguments)
        _completed_calls[tool_call_id] = (result, time.time())

    await ws.send(
        json.dumps(
            {
                "type": "tool_call_result",
                "payload": {
                    "tool_call_id": tool_call_id,
                    "result": result,
                },
            }
        )
    )
    logger.info(f"Sent result for {tool_call_id}")


async def connect():
    backoff = 1
    while True:
        try:
            logger.info(f"[{WORKER_NAME}] Connecting to {CONTROL_PLANE_URL}...")
            async with websockets.connect(CONTROL_PLANE_URL) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "type": "register",
                            "payload": {
                                "worker_name": WORKER_NAME,
                                "capabilities": CAPABILITIES,
                            },
                        }
                    )
                )

                reg_response = json.loads(await ws.recv())
                if reg_response.get("type") == "registered":
                    logger.info(
                        f"[{WORKER_NAME}] Registered with control plane"
                    )
                    backoff = 1
                else:
                    logger.warning(f"Unexpected registration response: {reg_response}")
                    continue

                heartbeat_task = asyncio.create_task(heartbeat_loop(ws))

                try:
                    async for raw_message in ws:
                        message = json.loads(raw_message)
                        if message.get("type") == "tool_call_request":
                            msg_id = message.get("message_id", "")
                            tc_id = message["payload"].get("tool_call_id", "")
                            await ws.send(json.dumps({
                                "type": "ack",
                                "message_id": msg_id,
                                "tool_call_id": tc_id,
                            }))
                            asyncio.create_task(
                                handle_tool_call(ws, message["payload"])
                            )
                        elif message.get("type") == "terminate":
                            reason = message.get("payload", {}).get("reason", "unknown")
                            logger.info(f"[{WORKER_NAME}] Received terminate: {reason}")
                            sys.exit(0)
                        elif message.get("type") == "heartbeat_ack":
                            pass
                        else:
                            logger.warning(f"Unknown message type: {message.get('type')}")
                finally:
                    heartbeat_task.cancel()

        except (
            websockets.exceptions.ConnectionClosed,
            ConnectionRefusedError,
            OSError,
        ) as e:
            logger.warning(f"[{WORKER_NAME}] Connection lost: {e}. Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def heartbeat_loop(ws):
    try:
        while True:
            await asyncio.sleep(15)
            await ws.send(json.dumps({"type": "heartbeat"}))
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    main_task = loop.create_task(connect())

    def _shutdown(sig, _frame):
        logger.info(f"[{WORKER_NAME}] Received {signal.Signals(sig).name}, shutting down...")
        main_task.cancel()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        loop.run_until_complete(main_task)
    except asyncio.CancelledError:
        logger.info(f"[{WORKER_NAME}] Clean shutdown complete")
    finally:
        loop.close()
