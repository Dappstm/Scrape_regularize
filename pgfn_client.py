# pgfn_client.py
from __future__ import annotations
import logging, random, math, asyncio
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from playwright.async_api import BrowserContext, Page, Route
from config import PGFN_BASE, WAIT_LONG

logger = logging.getLogger("PGFNClient")


@dataclass
class DebtorRow:
    cnpj: str
    inscriptions: Optional[List[str]] = None  # new field


def _to_float_safe(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        s = s.replace('.', '').replace(',', '.')
        return float(s)
    except Exception:
        return None


class PGFNClient:
    def __init__(self, context: BrowserContext):
        self.context = context
        self.page: Optional[Page] = None
        self._last_detail_json: Optional[Dict[str, Any]] = None
        self._auth_token: Optional[str] = None  # cache token

    # --- Human-like helpers ---
    async def _human_mouse_curve(self, page, start, end, steps=30, jitter=3):
        """
        Move mouse along a curved Bézier path with jitter.
        start, end = (x, y) tuples
        """
        # Random control points for cubic Bézier
        cx1 = start[0] + (end[0] - start[0]) * random.uniform(0.2, 0.5) + random.randint(-40, 40)
        cy1 = start[1] + (end[1] - start[1]) * random.uniform(0.2, 0.5) + random.randint(-40, 40)
        cx2 = start[0] + (end[0] - start[0]) * random.uniform(0.5, 0.8) + random.randint(-40, 40)
        cy2 = start[1] + (end[1] - start[1]) * random.uniform(0.5, 0.8) + random.randint(-40, 40)

        for t in [i / steps for i in range(steps + 1)]:
            # Cubic Bézier interpolation
            x = (1-t)**3 * start[0] + 3*(1-t)**2 * t * cx1 + 3*(1-t) * t**2 * cx2 + t**3 * end[0]
            y = (1-t)**3 * start[1] + 3*(1-t)**2 * t * cy1 + 3*(1-t) * t**2 * cy2 + t**3 * end[1]

            # Add jitter (tiny natural micro-movements)
            x += random.uniform(-jitter, jitter)
            y += random.uniform(-jitter, jitter)

            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.005, 0.02))  # natural reaction delay

    async def _human_type(self, p: Page, selector: str, text: str) -> None:
        """Type text with natural variable delays, occasional backspaces and small hesitations."""
        await p.focus(selector)
        await asyncio.sleep(random.uniform(0.05, 0.6))
        typed = ""
        for ch in text:
            if random.random() < 0.03 and len(text) - len(typed) > 3:
                chunk = text[len(typed): len(typed) + random.randint(2, 4)]
                await p.keyboard.insert_text(chunk)
                typed += chunk
                await asyncio.sleep(random.uniform(0.08, 0.25))
                continue
            await p.keyboard.type(ch, delay=random.randint(40, 160))
            typed += ch
            if random.random() < 0.06:
                await asyncio.sleep(random.uniform(0.12, 0.9))
            if random.random() < 0.02 and typed:
                await p.keyboard.press("Backspace")
                typed = typed[:-1]
                await asyncio.sleep(random.uniform(0.06, 0.2))
        await asyncio.sleep(random.uniform(0.15, 0.6))

    async def _human_scroll_and_view(self, p: Page) -> None:
        """Random short scrolls to simulate reading/inspection before interacting."""
        try:
            height = await p.evaluate(
                "() => Math.max(document.documentElement.scrollHeight, document.body.scrollHeight)"
            )
            for _ in range(random.randint(1, 3)):
                y = random.randint(0, max(10, int(0.4 * height)))
                await p.mouse.wheel(0, y)
                await asyncio.sleep(random.uniform(0.08, 0.5))
        except Exception:
            pass

    async def _bulletproof_click(self, selector: str, label: str) -> bool:
        """Try multiple strategies to click a button reliably."""
        assert self.page is not None
        p = self.page
        try:
            await p.click(selector)
            logger.info("[CLICK] %s (normal)", label)
            return True
        except Exception as e1:
            logger.warning("[CLICK] Normal click failed on %s: %s", label, e1)
            try:
                await p.click(selector, force=True)
                logger.info("[CLICK] %s (force)", label)
                return True
            except Exception as e2:
                logger.error("[CLICK] Force click failed on %s: %s", label, e2)
        return False

    async def open(self):
        """Open PGFN site."""
        self.page = await self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)
        logger.info("[PGFN] Opening base page: %s", PGFN_BASE)
        await self.page.goto(PGFN_BASE, wait_until="domcontentloaded")
        logger.info("[PGFN] Base page loaded.")
        
        # cookies = await self.context.cookies()
        # logger.info("[PGFN] Got %d cookies for session", len(cookies))

    async def search_company(self, name_query: str, max_attempts: int = 3) -> List[DebtorRow]:
        """
        Perform search (human-like) and reliably fetch /api/devedores response.
        Retries on 401 by attempting to refresh the token (via responses / localStorage / reloading).
        """
        assert self.page is not None
        p = self.page

        async def _extract_token_from_storage() -> Optional[str]:
            try:
                token = await p.evaluate(
                    "() => (window.localStorage.getItem('Authorization') || window.sessionStorage.getItem('Authorization') || null)"
                )
                return token
            except Exception:
                return None

        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            logger.info("[SEARCH] attempt %s/%s for query=%s", attempt, max_attempts, name_query)

            try:
                await p.unroute("**/api/devedores*")
            except Exception:
                pass
            try:
                await p.off("response", getattr(self, "_response_listener"))
            except Exception:
                pass

            async def _response_listener(resp):
                try:
                    url = resp.url.lower()
                    if "api/devedores" in url:
                        hdrs = {k.lower(): v for k, v in resp.headers.items()}
                        if "authorization" in hdrs:
                            self._auth_token = hdrs["authorization"]
                            logger.info("[AUTH] token updated from response header (authorization)")
                        # elif "total-control" in hdrs:
                            # self._auth_token = hdrs["total-control"]
                            # logger.info("[AUTH] token updated from response header (total-control)")
                        if "/api/devedores?id=" in url:
                            # Get the Playwright Request object that triggered this response
                            req = resp.request

                            # Make simple dict copies (headers may be a case-insensitive mapping)
                            req_headers = {k: v for k, v in req.headers.items()}
                            resp_headers = {k: v for k, v in resp.headers.items()}

                            # Mask Authorization for safety in logs
                            def _mask_token(hdrs, key="authorization"):
                                val = hdrs.get(key) or hdrs.get(key.capitalize())
                                if not val:
                                    return None
                                try:
                                    return val[:20] + "..."  # keep prefix
                                except Exception:
                                    return "<masked>"

                            logger.debug("[REQ] %s %s headers=%s", req.method, req.url, req_headers)
                            logger.debug("[REQ] Authorization (masked)=%s", _mask_token(req_headers))
                            logger.debug("[RESP] %s %s headers=%s", resp.status, resp.url, resp_headers)
                            logger.debug("[RESP] Authorization (masked)=%s", _mask_token(resp_headers))
                            try:
                                data = await resp.json()
                                self._last_detail_json = data
                                logger.info("[XHR] captured detail JSON for %s %s", resp.status, resp.url)
                            except Exception as e:
                                logger.debug("[XHR] detail json parsing failed: %s", e)
                except Exception as e:
                    logger.debug("[XHR] response listener error: %s", e)

            p.on("response", _response_listener)
            self._response_listener = _response_listener

            async def _route_handler(route: Route):
                try:
                    req = route.request
                    headers = req.headers.copy()
                    if self._auth_token:
                        headers["authorization"] = self._auth_token
                    await route.continue_(headers=headers)
                except Exception as e:
                    logger.debug("[ROUTE] continue_ failed: %s", e)
                    await route.continue_()

            await p.route("**/api/devedores*", _route_handler)

            await self._human_scroll_and_view(p)

            await asyncio.sleep(random.uniform(0.5, 1.5))  # pause as if reading

            viewport = p.viewport_size
            if viewport:
                await p.mouse.move(viewport["width"] + random.randint(20, 80),
                                   random.randint(50, viewport["height"] - 50))
                await asyncio.sleep(random.uniform(0.3, 1.0))

            await self._human_type(p, "input#nome, input[formcontrolname='nome']", name_query)
            await asyncio.sleep(random.uniform(0.6, 1.8))  # hesitation

            btn = await p.query_selector("button:has-text('Consultar'), button.btn.btn-warning")
            if not btn:
                logger.error("[SEARCH] Consultar button not found")
                return []

            box = await btn.bounding_box()
            if box:
                # Random exploratory hovers around the button
                for _ in range(random.randint(1, 2)):
                    hover_x = box["x"] + random.uniform(0, box["width"])
                    hover_y = box["y"] + random.uniform(0, box["height"])
                    await p.mouse.move(hover_x, hover_y, steps=random.randint(8, 14))
                    await asyncio.sleep(random.uniform(0.2, 0.6))

                # Curved, jittery movement to center
                start = (random.uniform(10, 200), random.uniform(100, 400))
                end = (box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                await self._human_mouse_curve(p, start, end, steps=random.randint(24, 42))

                await asyncio.sleep(random.uniform(0.25, 0.8))  # hover pause before click

            try:
                async with p.expect_response(lambda r: "/api/devedores" in r.url.lower(),
                                             timeout=30000) as resp_ctx:
                    await btn.click()
                resp = await resp_ctx.value
            except Exception as e:
                logger.warning("[SEARCH] timeout waiting for /api/devedores: %s", e)
                if attempt < max_attempts:
                    logger.info("[SEARCH] Reloading base page and retrying...")
                    await p.goto(PGFN_BASE, wait_until="domcontentloaded")
                    await asyncio.sleep(random.uniform(0.5, 1.2))
                    continue
                return []

            logger.info("[SEARCH] API responded %s for %s", resp.status, resp.url)

            if resp.status == 401:
                logger.warning("[SEARCH] 401 -> refreshing token (attempt %s/%s)", attempt, max_attempts)
                hdrs = {k.lower(): v for k, v in resp.headers.items()}
                new_token = hdrs.get("authorization") or hdrs.get("total-control")
                if new_token:
                    self._auth_token = new_token
                    logger.info("[AUTH] refreshed token from response headers")
                    continue
                store_token = await _extract_token_from_storage()
                if store_token:
                    self._auth_token = store_token
                    logger.info("[AUTH] refreshed token from local/sessionStorage")
                    continue
                if attempt < max_attempts:
                    logger.info("[SEARCH] Reloading page to force token refresh...")
                    await p.goto(PGFN_BASE, wait_until="domcontentloaded")
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    maybe = await _extract_token_from_storage()
                    if maybe:
                        self._auth_token = maybe
                        logger.info("[AUTH] got token after reload")
                    continue
                logger.error("[SEARCH] exhausted attempts -> 401")
                return []

            if resp.ok:
                try:
                    await p.wait_for_selector("p.total-mensagens.info-panel", timeout=60000)
                except Exception:
                    pass
                rows = await p.query_selector_all("table tbody tr")
                logger.info("[SEARCH] Found %d rows", len(rows))

                debtors: List[DebtorRow] = []
                for idx, row in enumerate(rows, 1):
                    try:
                        detail_btn = await row.query_selector("i.ion-ios-open, button[title*='Detalhar']")
                        if not detail_btn:
                            continue

                        self._last_detail_json = None

                        # --- Human-like pre-scroll before interacting ---
                        await self._human_scroll_and_view(p)
                        await asyncio.sleep(random.uniform(0.4, 1.6))  # pause as if reading row details

                        # Move mouse off-row and back (like repositioning to focus)
                        viewport = p.viewport_size
                        if viewport:
                            await p.mouse.move(viewport["width"] + random.randint(10, 60),
                                               random.randint(40, viewport["height"] - 40))
                            await asyncio.sleep(random.uniform(0.25, 0.9))

                        # Get button box
                        box = await detail_btn.bounding_box()
                        if box:
                            # Start from current "neutral" center position
                            cur = await p.evaluate(
                                "() => ({x: window.scrollX + (window.innerWidth/2), y: window.scrollY + (window.innerHeight/2)})"
                            )
                            target = (box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)

                            # Random exploratory hovers around the button edges (human hesitation)
                            for _ in range(random.randint(1, 3)):
                                hover_x = box["x"] + random.uniform(0, box["width"])
                                hover_y = box["y"] + random.uniform(0, box["height"])
                                await p.mouse.move(hover_x, hover_y, steps=random.randint(6, 12))
                                await asyncio.sleep(random.uniform(0.2, 0.7))

                            # Overshoot path then correction
                            overshoot = (
                                target[0] + random.uniform(-6, 12),
                                target[1] + random.uniform(-6, 12),
                            )
                            await self._human_mouse_curve(p, (cur["x"], cur["y"]), overshoot,
                                                          steps=random.randint(14, 28))
                            await asyncio.sleep(random.uniform(0.5, 2.0))
                            await self._human_mouse_curve(p, overshoot, target,
                                                          steps=random.randint(6, 12))

                            # Pause like “reading tooltip” before committing
                            await asyncio.sleep(random.uniform(0.6, 2.4))

                        # Final click
                        await detail_btn.click()

                        # Post-click behaviors: simulate viewing modal
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                        await self._human_scroll_and_view(p)
                        await asyncio.sleep(random.uniform(0.3, 0.8))
                        for _ in range(40):
                            if self._last_detail_json:
                                break
                            await asyncio.sleep(0.5)
                        if not self._last_detail_json:
                            continue
                        data_detail = self._last_detail_json
                        cnpj = str(data_detail.get("id") or "").strip()
                        inscriptions: List[str] = []
                        try:
                            for nat in data_detail.get("naturezas", []) or []:
                                for deb in nat.get("debitos", []) or []:
                                    if deb.get("numero"):
                                        inscriptions.append(str(deb["numero"]).strip())
                        except Exception:
                            pass
                        debtors.append(DebtorRow(cnpj=cnpj, inscriptions=inscriptions))
                        logger.info("[ROW] %s -> %d inscriptions", cnpj, len(inscriptions))
                        close_btn = await p.query_selector("button.close, .modal .btn-close")
                        if close_btn:
                            await close_btn.click()
                            await asyncio.sleep(random.uniform(0.2, 0.9))
                    except Exception as row_err:
                        logger.error("[ROW] Error row %s: %s", idx, row_err)

                unique = {}
                for d in debtors:
                    if d.cnpj not in unique:
                        unique[d.cnpj] = d
                return list(unique.values())

        logger.error("[SEARCH] Exhausted all attempts without success")
        return []