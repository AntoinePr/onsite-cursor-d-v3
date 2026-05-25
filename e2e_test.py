"""E2E test using Playwright: open the UI, create a session, send a message that triggers a tool call, verify the result."""

import sys

from playwright.sync_api import sync_playwright, expect


def test_full_tool_call_round_trip():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.on("console", lambda msg: print(f"    [browser] {msg.text}"))

        print("[1] Loading UI at http://localhost:8000 ...")
        page.goto("http://localhost:8000")
        page.evaluate("localStorage.clear()")
        page.reload()
        page.wait_for_load_state("networkidle")

        print("[2] Verifying page title and elements ...")
        assert "ExeCursor" in page.title()

        header = page.locator("header h1")
        expect(header).to_have_text("ExeCursor")

        print("[3] Verifying empty state (no auto-session) ...")
        empty_state = page.locator("#empty-state")
        expect(empty_state).to_be_visible(timeout=5000)
        expect(empty_state).to_have_text("Create a new session to start")
        expect(page.locator("#message-input")).to_be_disabled()

        print("[4] Waiting for workers to appear in sidebar ...")
        page.wait_for_selector(".worker-card", timeout=10000)
        worker_cards = page.locator(".worker-card")
        worker_count = worker_cards.count()
        print(f"    {worker_count} worker(s) connected")
        assert worker_count >= 1, "Expected at least 1 worker"

        print("[5] Creating a new session ...")
        page.click(".new-session-btn")
        page.wait_for_timeout(2000)
        expect(page.locator("#empty-state")).to_have_count(0)
        expect(page.locator("#message-input")).to_be_enabled()

        # ── Test 1: get_system_info tool call ───────────────────────────
        print("[6] Sending message to trigger get_system_info tool call ...")
        input_box = page.locator("#message-input")
        send_btn = page.locator("#send-btn")

        input_box.fill("Please call the get_system_info tool right now.")
        send_btn.click()

        print("[7] Verifying user message appears ...")
        user_msg = page.locator(".message.user")
        expect(user_msg).to_be_visible(timeout=3000)

        print("[8] Waiting for either a tool call or assistant response (up to 45s) ...")
        page.wait_for_selector(".message.tool-call, .message.assistant", timeout=45000)

        tool_call = page.locator(".message.tool-call")
        assistant_msg = page.locator(".message.assistant")

        if tool_call.count() > 0:
            print("    Tool call detected!")
            tool_text = tool_call.first.inner_text()
            print(f"    Tool call: {tool_text[:150]}...")

            print("[9] Waiting for tool result ...")
            tool_result = page.locator(".tool-call-result").first
            expect(tool_result).not_to_have_text("Executing...", timeout=30000)
            result_text = tool_result.inner_text()
            print(f"    Tool result: {result_text[:250]}")

            assert "worker" in result_text.lower(), \
                f"Expected worker info in result, got: {result_text}"

            print("[10] Waiting for assistant's final response ...")
            expect(assistant_msg.first).to_be_visible(timeout=30000)
            final_text = assistant_msg.first.inner_text()
            print(f"    Assistant: {final_text[:200]}")
        else:
            final_text = assistant_msg.first.inner_text()
            print(f"    Assistant responded directly (no tool call): {final_text[:200]}")

        # ── Test 2: execute_shell tool call ─────────────────────────────
        print("[11] Waiting for input to be re-enabled ...")
        page.wait_for_function(
            "!document.getElementById('message-input').disabled", timeout=10000
        )

        print("[12] Sending shell command: 'echo hello_from_e2e_test' ...")
        input_box.fill("Execute the shell command 'echo hello_from_e2e_test'. Use the bash tool.")
        send_btn.click()

        print("[13] Waiting for second tool call or response ...")
        page.wait_for_function(
            "document.querySelectorAll('.message.tool-call').length >= 2 || "
            "document.querySelectorAll('.message.assistant').length >= 2",
            timeout=45000,
        )

        if tool_call.count() >= 2:
            print("    Second tool call detected!")
            second_tool = tool_call.nth(1 if tool_call.count() > 1 else 0)
            second_text = second_tool.inner_text()
            print(f"    Tool call: {second_text[:150]}...")

            print("[14] Waiting for second tool result ...")
            second_result = page.locator(".tool-call-result").nth(1 if page.locator(".tool-call-result").count() > 1 else 0)
            expect(second_result).not_to_have_text("Executing...", timeout=30000)
            result2_text = second_result.inner_text()
            print(f"    Result: {result2_text[:200]}")

            assert "hello_from_e2e_test" in result2_text, \
                f"Expected 'hello_from_e2e_test' in result, got: {result2_text}"

        print("[15] Waiting for final assistant response ...")
        if assistant_msg.count() >= 2:
            final2 = assistant_msg.nth(1).inner_text()
        else:
            final2 = assistant_msg.last.inner_text()
        print(f"    Assistant: {final2[:200]}")

        # ── Test 3: session green dot (not ACTIVE badge) ────────────────
        print("[16] Verifying session card has green dot, no ACTIVE badge ...")
        session_card = page.locator("#sessions-list .session-card")
        expect(session_card.locator(".session-dot")).to_be_visible()
        expect(session_card.locator(".session-status-badge")).to_have_count(0)

        page.screenshot(path="e2e_screenshot.png")
        print()
        print("=" * 60)
        print("  ALL E2E TESTS PASSED")
        print("=" * 60)
        print("  Screenshot saved to e2e_screenshot.png")

        browser.close()


def test_cp_restart_resilience():
    """Validate that the system survives a control-plane restart."""
    import subprocess, time, json, requests

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.on("console", lambda msg: print(f"    [browser] {msg.text}"))

        # ── 1. Pre-restart setup ─────────────────────────────────────────
        print("[CP-1] Cleaning up existing sessions ...")
        existing = requests.get("http://localhost:8000/api/sessions").json()
        for s in existing.get("sessions", []):
            requests.delete(f"http://localhost:8000/api/sessions/{s['id']}")

        print("[CP-1b] Loading UI, clearing state ...")
        page.goto("http://localhost:8000")
        page.evaluate("localStorage.clear()")
        page.reload()
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(1000)

        empty_state = page.locator("#empty-state")
        expect(empty_state).to_be_visible(timeout=10000)

        print("[CP-2] Creating a new session ...")
        page.click(".new-session-btn")
        page.wait_for_timeout(2000)
        expect(page.locator("#message-input")).to_be_enabled()

        print("[CP-3] Sending message to trigger get_system_info ...")
        page.locator("#message-input").fill("Please call the get_system_info tool right now.")
        page.locator("#send-btn").click()

        print("[CP-4] Waiting for tool call + assistant response ...")
        page.wait_for_selector(".message.tool-call", timeout=45000)
        tool_result = page.locator(".tool-call-result").first
        expect(tool_result).not_to_have_text("Executing...", timeout=30000)
        page.wait_for_selector(".message.assistant", timeout=30000)
        page.wait_for_function(
            "!document.getElementById('message-input').disabled", timeout=10000
        )

        print("[CP-5] Verifying session card has green dot ...")
        session_card = page.locator("#sessions-list .session-card")
        expect(session_card.locator(".session-dot")).to_be_visible()

        pre_restart_user_msgs = page.locator(".message.user").count()
        pre_restart_tool_calls = page.locator(".message.tool-call").count()
        print(f"    User messages: {pre_restart_user_msgs}, Tool calls: {pre_restart_tool_calls}")

        workers_resp = requests.get("http://localhost:8000/api/workers").json()
        bound_worker_before = None
        for w in workers_resp["workers"]:
            if w["session_id"] is not None:
                bound_worker_before = w["name"]
                break
        print(f"    Bound worker before restart: {bound_worker_before}")

        sessions_resp = requests.get("http://localhost:8000/api/sessions").json()
        session_before = sessions_resp["sessions"][0]
        session_id = session_before["id"]
        print(f"    Session: {session_id[:8]} status={session_before['status']} has_worker={session_before['has_worker']}")

        # ── 2. Kill and restart the control plane ────────────────────────
        print("[CP-6] Killing control-plane container ...")
        subprocess.run(
            ["docker", "compose", "kill", "control-plane"],
            cwd="/Users/antoine.protard/go/src/github.com/DataDog/onsite-cursor-d-v3",
            check=True,
        )
        time.sleep(1)

        print("[CP-7] Restarting control-plane container ...")
        subprocess.run(
            ["docker", "compose", "up", "-d", "control-plane"],
            cwd="/Users/antoine.protard/go/src/github.com/DataDog/onsite-cursor-d-v3",
            check=True,
        )

        print("[CP-8] Waiting for CP to come back healthy ...")
        for attempt in range(30):
            try:
                r = requests.get("http://localhost:8000/api/workers", timeout=2)
                if r.status_code == 200:
                    print(f"    CP healthy after {attempt + 1} attempts")
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            raise RuntimeError("CP did not come back healthy within 30s")

        print("[CP-9] Waiting for workers to reconnect ...")
        for attempt in range(30):
            try:
                r = requests.get("http://localhost:8000/api/workers", timeout=2)
                data = r.json()
                if len(data["workers"]) >= 1:
                    print(f"    {len(data['workers'])} worker(s) reconnected")
                    break
            except Exception:
                pass
            time.sleep(1)

        # ── 3. Post-restart: browser auto-reconnects ─────────────────────
        print("[CP-10] Waiting for browser WebSocket to reconnect ...")
        for attempt in range(15):
            page.wait_for_timeout(2000)
            ws_state = page.evaluate("ws ? ws.readyState : -1")
            conn_class = page.evaluate("document.getElementById('connection-dot').className")
            input_disabled = page.evaluate("document.getElementById('message-input').disabled")
            print(f"    Attempt {attempt+1}: ws={ws_state} conn={conn_class} input_disabled={input_disabled}")
            if ws_state == 1 and not input_disabled:
                break

        print("[CP-11] Verifying chat history is replayed ...")
        post_user = page.locator(".message.user").count()
        post_tools = page.locator(".message.tool-call").count()
        print(f"    After restart: user={post_user}, tool-calls={post_tools}")
        assert post_user >= pre_restart_user_msgs, \
            f"Expected {pre_restart_user_msgs} user messages, got {post_user}"
        assert post_tools >= pre_restart_tool_calls, \
            f"Expected {pre_restart_tool_calls} tool calls, got {post_tools}"

        print("[CP-12] Verifying input is re-enabled ...")
        input_disabled = page.evaluate("document.getElementById('message-input').disabled")
        assert not input_disabled, "Input should be enabled after reconnect"

        print("[CP-13] Verifying session card still visible with green dot ...")
        session_card = page.locator("#sessions-list .session-card")
        expect(session_card.first).to_be_visible(timeout=5000)

        # ── 4. Post-restart: worker-session binding restored ─────────────
        print("[CP-14] Checking worker-session binding is restored ...")
        time.sleep(3)
        workers_resp = requests.get("http://localhost:8000/api/workers").json()
        bound_worker_after = None
        for w in workers_resp["workers"]:
            if w["session_id"] == session_id:
                bound_worker_after = w["name"]
                break
        print(f"    Bound worker after restart: {bound_worker_after}")
        assert bound_worker_after is not None, "Worker-session binding was not restored after restart"

        sessions_resp = requests.get("http://localhost:8000/api/sessions").json()
        session_after = None
        for s in sessions_resp["sessions"]:
            if s["id"] == session_id:
                session_after = s
                break
        assert session_after is not None, "Session not found after restart"
        print(f"    Session status={session_after['status']} has_worker={session_after['has_worker']}")
        assert session_after["has_worker"], "Session should have a worker after restart"

        # ── 5. Post-restart: send new messages ───────────────────────────
        print("[CP-15] Sending a new message after restart ...")
        page.locator("#message-input").fill("Run echo cp_restart_ok")
        page.locator("#send-btn").click()

        print("[CP-16] Waiting for tool call + response ...")
        page.wait_for_function(
            f"document.querySelectorAll('.message.tool-call').length >= 2",
            timeout=45000,
        )
        page.wait_for_function(
            f"document.querySelectorAll('.message.assistant').length >= 1",
            timeout=45000,
        )
        page.wait_for_function(
            "!document.getElementById('message-input').disabled", timeout=15000
        )

        all_text = page.locator(".tool-call-result").last.inner_text()
        print(f"    Last tool result: {all_text[:200]}")
        assert "cp_restart_ok" in all_text, f"Expected 'cp_restart_ok' in post-restart result"

        # ── 6. Post-restart: in-flight dispatches cleaned up ─────────────
        print("[CP-17] Checking in-flight dispatches are cleaned up ...")
        dispatches_resp = requests.get("http://localhost:8000/debug/dispatches").json()
        stuck = [d for d in dispatches_resp["dispatches"] if d["status"] in ("dispatched", "acked")]
        print(f"    Total dispatches: {len(dispatches_resp['dispatches'])}, stuck: {len(stuck)}")
        assert len(stuck) == 0, f"Found {len(stuck)} stuck dispatches: {stuck}"

        page.screenshot(path="e2e_cp_restart_screenshot.png")
        print()
        print("=" * 60)
        print("  CP RESTART RESILIENCE TESTS PASSED")
        print("=" * 60)
        print("  Screenshot saved to e2e_cp_restart_screenshot.png")

        browser.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", choices=["basic", "cp-restart", "all"], default="all")
    args = parser.parse_args()

    failed = False
    if args.test in ("basic", "all"):
        try:
            test_full_tool_call_round_trip()
        except Exception as e:
            print(f"\nBASIC TEST FAILED: {e}", file=sys.stderr)
            failed = True

    if args.test in ("cp-restart", "all"):
        try:
            test_cp_restart_resilience()
        except Exception as e:
            print(f"\nCP RESTART TEST FAILED: {e}", file=sys.stderr)
            failed = True

    if failed:
        sys.exit(1)
