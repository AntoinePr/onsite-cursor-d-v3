import asyncio
import json
import logging
import os
import platform
import subprocess
import time

import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "ws://localhost:8000/ws/worker")
WORKER_NAME = os.getenv("WORKER_NAME", "worker-unknown")

CAPABILITIES = ["execute_shell", "get_system_info"]


def execute_shell(command: str) -> str:
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
    "execute_shell": lambda args: execute_shell(args.get("command", "echo 'no command'")),
    "get_system_info": lambda args: get_system_info(),
}


async def handle_tool_call(ws, payload: dict):
    tool_call_id = payload["tool_call_id"]
    function_name = payload["function_name"]
    arguments = payload.get("arguments", {})

    logger.info(f"Executing {function_name} (id={tool_call_id})")

    handler = TOOL_HANDLERS.get(function_name)
    if handler is None:
        result = json.dumps({"error": f"Unknown function: {function_name}"})
    else:
        result = await asyncio.get_event_loop().run_in_executor(None, handler, arguments)

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
                            asyncio.create_task(
                                handle_tool_call(ws, message["payload"])
                            )
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
    asyncio.run(connect())
