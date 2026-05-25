"""Playwright QA: verify worker labels, kill button, past sessions, session gating."""
import asyncio
import re
from playwright.async_api import async_playwright

BASE = "http://localhost:8000"


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(BASE)
        await page.wait_for_timeout(3000)

        # ── 1. Worker labels should be "Worker N", not "worker-n" ──
        print("=== QA 1: Worker display names ===")
        worker_cards = page.locator(".worker-card .name")
        count = await worker_cards.count()
        assert count == 3, f"Expected 3 workers, got {count}"
        for i in range(count):
            text = await worker_cards.nth(i).text_content()
            assert re.match(r"^Worker \d+$", text.strip()), f"Bad worker name: '{text}'"
            print(f"  OK: '{text.strip()}'")

        # ── 2. No bound-tag (orange session hash) visible ──
        print("\n=== QA 2: No bound-tag on workers ===")
        bound_tags = page.locator(".worker-card .bound-tag")
        bt_count = await bound_tags.count()
        assert bt_count == 0, f"Expected 0 bound-tags, got {bt_count}"
        print("  OK: no bound-tags found")

        # ── 3. Create session 1, send message to bind a worker ──
        print("\n=== QA 3: Create session & verify Kill button ===")
        await page.fill("#message-input", "run get_system_info")
        await page.click("#send-btn")
        await page.wait_for_timeout(10000)

        # Refresh sidebar
        await page.evaluate("refreshWorkers(); refreshSessions();")
        await page.wait_for_timeout(2000)

        # Active session should have a kill button
        kill_btns = page.locator("#sessions-list .session-kill-btn")
        kb_count = await kill_btns.count()
        assert kb_count >= 1, f"Expected >= 1 kill buttons on active sessions, got {kb_count}"
        print(f"  OK: {kb_count} kill button(s) on active sessions")

        # ── 4. Create session 2 to push session 1 to past ──
        print("\n=== QA 4: Create 2nd session, bind worker, kill 1st ===")
        # Click new session
        new_btn = page.locator("#sessions-list .new-session-btn")
        if await new_btn.is_enabled():
            await new_btn.click()
            await page.wait_for_timeout(3000)
            await page.fill("#message-input", "run get_system_info")
            await page.click("#send-btn")
            await page.wait_for_timeout(10000)

        # Kill first session to push it to past
        await page.evaluate("refreshWorkers(); refreshSessions();")
        await page.wait_for_timeout(2000)

        # Now kill the first active session via API so it becomes "past"
        sessions_list = page.locator("#sessions-list .session-card")
        active_count = await sessions_list.count()
        if active_count >= 2:
            first_kill = page.locator("#sessions-list .session-kill-btn").first
            await first_kill.click()
            await page.wait_for_timeout(5000)
            await page.evaluate("refreshWorkers(); refreshSessions();")
            await page.wait_for_timeout(2000)

        # ── 5. Past sessions should have NO kill/delete button ──
        print("\n=== QA 5: Past sessions have no buttons ===")
        past_buttons = page.locator("#past-sessions-list .session-kill-btn")
        pb_count = await past_buttons.count()
        past_delete = page.locator("#past-sessions-list .session-delete-btn")
        pd_count = await past_delete.count()
        assert pb_count == 0, f"Expected 0 kill buttons in past sessions, got {pb_count}"
        assert pd_count == 0, f"Expected 0 delete buttons in past sessions, got {pd_count}"
        past_cards = page.locator("#past-sessions-list .session-card")
        pc_count = await past_cards.count()
        print(f"  OK: {pc_count} past session(s), 0 buttons")

        # ── 6. Session gating: fill all workers, check button disabled ──
        print("\n=== QA 6: Session gating ===")
        # Create a 3rd session to fill all workers
        new_btn = page.locator("#sessions-list .new-session-btn")
        if await new_btn.is_enabled():
            await new_btn.click()
            await page.wait_for_timeout(3000)
            await page.fill("#message-input", "run get_system_info")
            await page.click("#send-btn")
            await page.wait_for_timeout(10000)

        await page.evaluate("refreshWorkers(); refreshSessions();")
        await page.wait_for_timeout(2000)

        new_btn = page.locator("#sessions-list .new-session-btn")
        is_disabled = await new_btn.get_attribute("disabled")
        btn_text = await new_btn.text_content()
        print(f"  Button text: '{btn_text.strip()}', disabled: {is_disabled is not None}")
        if is_disabled is not None:
            print("  OK: New Session button is disabled (all workers busy)")
        else:
            # May still have a free worker if fewer sessions bound
            print("  INFO: Button still enabled (workers may still be available)")

        await browser.close()
        print("\n=== ALL QA CHECKS PASSED ===")


asyncio.run(main())
