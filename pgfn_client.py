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
    async def _human_mouse_move(self, p: Page, start: tuple, end: tuple, steps: int = 30) -> None:
        """Move mouse along a slightly randomized quadratic Bezier curve."""
        x1, y1 = start
        x2, y2 = end
        cx = (x1 + x2) / 2 + random.uniform(-80, 80)
        cy = (y1 + y2) / 2 + random.uniform(-80, 80)
        for i in range(steps + 1):
            t = i / steps
            xt = (1 - t) ** 2 * x1 + 2 * (1 - t) * t * cx + t ** 2 * x2
            yt = (1 - t) ** 2 * y1 + 2 * (1 - t) * t * cy + t ** 2 * y2
            jitter_x = random.uniform(-1.2, 1.2)
            jitter_y = random.uniform(-1.2, 1.2)
            await p.mouse.move(xt + jitter_x, yt + jitter_y, steps=1)
            await asyncio.sleep(random.uniform(0.008, 0.04))

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
        cookies = await self.context.cookies()
        logger.info("[PGFN] Got %d cookies for session", len(cookies))

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
                        elif "total-control" in hdrs:
                            self._auth_token = hdrs["total-control"]
                            logger.info("[AUTH] token updated from response header (total-control)")
                        if "/api/devedores?id=" in url:
                            try:
                                data = await resp.json()
                                self._last_detail_json = data
                                logger.info("[XHR] captured detail JSON for %s", resp.url)
                            except Exception as e:
                                logger.debug("[XHR] detail json parsing failed: %s", e)
                except Exception as e:
                    logger.debug("[XHR] response listener error: %s", e)

            p.on("response", _response_listener)
            self._response_listener = _response_listener

            async def _route_handler(route: Route):
                try:
                    req = route.request
                    headers = dict(req.headers)
                    if self._auth_token:
                        headers["authorization"] = self._auth_token
                    await route.continue_(headers=headers)
                except Exception as e:
                    logger.debug("[ROUTE] continue_ failed: %s", e)
                    await route.continue_()

            await p.route("**/api/devedores*", _route_handler)

            await self._human_scroll_and_view(p)
            await asyncio.sleep(random.uniform(0.12, 0.6))
            await self._human_type(p, "input#nome, input[formcontrolname='nome']", name_query)
            await asyncio.sleep(random.uniform(0.25, 0.9))

            btn = await p.query_selector("button:has-text('Consultar'), button.btn.btn-warning")
            if not btn:
                logger.error("[SEARCH] Consultar button not found")
                return []

            box = await btn.bounding_box()
            if box:
                start = (random.uniform(10, 100), random.uniform(100, 300))
                end = (box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                await self._human_mouse_move(p, start, end, steps=random.randint(18, 34))

            try:
                async with p.expect_response(lambda r: "/api/devedores/" in r.url.lower(), timeout=30000) as resp_ctx:
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
                        box = await detail_btn.bounding_box()
                        if box:
                            cur = await p.evaluate("() => ({x: window.scrollX + (window.innerWidth/2), y: window.scrollY + (window.innerHeight/2)})")
                            await self._human_mouse_move(
                                p,
                                (cur["x"], cur["y"]),
                                (box["x"] + box["width"] / 2, box["y"] + box["height"] / 2),
                                steps=random.randint(14, 28),
                            )
                        await detail_btn.click()
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