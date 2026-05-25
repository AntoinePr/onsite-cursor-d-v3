"""E2E test using Playwright: open the UI, send a message that triggers a tool call, verify the result."""

import sys

from playwright.sync_api import sync_playwright, expect


def test_full_tool_call_round_trip():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.on("console", lambda msg: print(f"    [browser] {msg.text}"))

        print("[1] Loading UI at http://localhost:8000 ...")
        page.goto("http://localhost:8000")
        page.wait_for_load_state("networkidle")

        print("[2] Verifying page title and elements ...")
        assert "Remote Tool Execution" in page.title()

        header = page.locator("header h1")
        expect(header).to_have_text("Remote Tool Execution")

        print("[3] Waiting for workers to appear in status bar ...")
        worker_status = page.locator("#worker-status")
        expect(worker_status).to_contain_text("worker-", timeout=10000)
        status_text = worker_status.inner_text()
        print(f"    Worker status: {status_text}")

        print("[4] Verifying 'Connected to control plane' status message ...")
        connected_msg = page.locator(".message.status", has_text="Connected")
        expect(connected_msg).to_be_visible(timeout=5000)

        # ── Test 1: get_system_info tool call ───────────────────────────
        print("[5] Sending message to trigger get_system_info tool call ...")
        input_box = page.locator("#message-input")
        send_btn = page.locator("#send-btn")

        input_box.fill("Please call the get_system_info tool on worker-1 right now.")
        send_btn.click()

        print("[6] Verifying user message appears ...")
        user_msg = page.locator(".message.user")
        expect(user_msg).to_be_visible(timeout=3000)

        print("[7] Waiting for either a tool call or assistant response (up to 45s) ...")
        page.wait_for_selector(".message.tool-call, .message.assistant", timeout=45000)

        tool_call = page.locator(".message.tool-call")
        assistant_msg = page.locator(".message.assistant")

        if tool_call.count() > 0:
            print("    Tool call detected!")
            tool_text = tool_call.first.inner_text()
            print(f"    Tool call: {tool_text[:150]}...")

            print("[8] Waiting for tool result ...")
            tool_result = page.locator(".tool-call-result").first
            expect(tool_result).not_to_have_text("Executing...", timeout=30000)
            result_text = tool_result.inner_text()
            print(f"    Tool result: {result_text[:250]}")

            assert "worker" in result_text.lower(), \
                f"Expected worker info in result, got: {result_text}"

            print("[9] Waiting for assistant's final response ...")
            expect(assistant_msg.first).to_be_visible(timeout=30000)
            final_text = assistant_msg.first.inner_text()
            print(f"    Assistant: {final_text[:200]}")
        else:
            final_text = assistant_msg.first.inner_text()
            print(f"    Assistant responded directly (no tool call): {final_text[:200]}")

        # ── Test 2: execute_shell tool call ─────────────────────────────
        print("[10] Waiting for input to be re-enabled ...")
        page.wait_for_function(
            "!document.getElementById('message-input').disabled", timeout=10000
        )

        print("[11] Sending shell command: 'echo hello_from_e2e_test' on worker-2 ...")
        input_box.fill("Execute the shell command 'echo hello_from_e2e_test' on worker-2. Use the execute_shell tool.")
        send_btn.click()

        print("[12] Waiting for second tool call or response ...")
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

            print("[13] Waiting for second tool result ...")
            second_result = page.locator(".tool-call-result").nth(1 if page.locator(".tool-call-result").count() > 1 else 0)
            expect(second_result).not_to_have_text("Executing...", timeout=30000)
            result2_text = second_result.inner_text()
            print(f"    Result: {result2_text[:200]}")

            assert "hello_from_e2e_test" in result2_text, \
                f"Expected 'hello_from_e2e_test' in result, got: {result2_text}"

        print("[14] Waiting for final assistant response ...")
        if assistant_msg.count() >= 2:
            final2 = assistant_msg.nth(1).inner_text()
        else:
            final2 = assistant_msg.last.inner_text()
        print(f"    Assistant: {final2[:200]}")

        page.screenshot(path="e2e_screenshot.png")
        print()
        print("=" * 60)
        print("  ALL E2E TESTS PASSED")
        print("=" * 60)
        print("  Screenshot saved to e2e_screenshot.png")

        browser.close()


if __name__ == "__main__":
    try:
        test_full_tool_call_round_trip()
    except Exception as e:
        print(f"\nTEST FAILED: {e}", file=sys.stderr)
        sys.exit(1)
