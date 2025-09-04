# pgfn_client.py
from __future__ import annotations
import logging, random
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from playwright.async_api import BrowserContext, Page
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

    async def search_company(self, name_query: str) -> List[DebtorRow]:
        """Perform search, parse results table, and expand each row to collect inscriptions."""
        assert self.page is not None
        p = self.page

        # Fill search form
        await p.wait_for_timeout(random.randint(1000, 2000))
        await p.fill("input#nome, input[formcontrolname='nome']", name_query)
        await p.wait_for_timeout(random.randint(1000, 2000))
        
        # --- Global request/response debugging ---

        async def handle_request(req):
            # Log all API calls (narrow it down if too noisy)
            if "api/devedores" in req.url:
                logger.info("[REQ] %s %s", req.method, req.url)
                logger.info("[REQ] headers=%s", req.headers)
                try:
                    logger.info("[REQ] post_data=%s", req.post_data)
                except Exception:
                    pass

        async def handle_response(resp):
            if "api/devedores" in resp.url:
                logger.info("[RESP] %s %s", resp.status, resp.url)
                logger.info("[RESP] headers=%s", resp.headers)
                try:
                    data = await resp.json()
                    logger.info("[RESP] JSON keys: %s", list(data.keys()))
                except Exception:
                    # sometimes not JSON
                    text = await resp.text()
                    logger.info("[RESP] text sample: %s", text[:200])
                    pass

        p.on("request", handle_request)
        p.on("response", handle_response)

        async with p.expect_response(lambda r: "/api/devedores" in r.url) as resp_info:
            await self._bulletproof_click(
                "button:has-text('Consultar'), button.btn.btn-warning",
                "CONSULTAR",
            )

        resp = await resp_info.value
        logger.info("[SEARCH] API responded %s for %s", resp.status, resp.url)

        if not resp.ok:
            logger.error("[SEARCH] API request failed with %s", resp.status)
            return []

        # Now wait for the "total-mensagens" element that shows result count
        await p.wait_for_selector("p.total-mensagens.info-panel", state="attached", timeout=60000)

        # Parse table rows
        rows = await p.query_selector_all("table tbody tr")
        logger.info("[SEARCH] Found %d result rows", len(rows))

        debtors: List[DebtorRow] = []

        for idx, row in enumerate(rows, 1):
            try:
                # Locate Detalhar button
                detail_btn = await row.query_selector("i.ion.ion-ios-open")
                if not detail_btn:
                    logger.warning("[DETAIL] No Detalhar button for row %s", idx)
                    continue

                # Reset and listen for detail XHR
                self._last_detail_json = None
                
                # Log outgoing request headers for debugging
                async def handle_request(req):
                    if "/api/devedores?id=" in req.url:
                        logger.info("[REQ] %s %s", req.method, req.url)
                        logger.info("[REQ] headers=%s", req.headers)

                p.on("request", handle_request)

                async def handle_response(resp):
                    if "/api/devedores?id=" in resp.url:
                        logger.info("[XHR] %s %s", resp.status, resp.url)
                        
                        try:
                            data = await resp.json()
                            self._last_detail_json = data
                            logger.info("[DETAIL] Captured JSON for row %s", idx)
                        except Exception as e:
                            logger.error("[DETAIL] Failed parsing JSON: %s", e)

                p.on("response", handle_response)

                # Click detail button
                await detail_btn.click()
                await p.wait_for_timeout(30000)  # allow modal + XHR

                # Wait until JSON captured or timeout
                for _ in range(30):
                    if self._last_detail_json:
                        break
                    await p.wait_for_timeout(5000)

                if not self._last_detail_json:
                    logger.error("[ROW] No JSON captured for row %s", idx)
                    continue

                # Extract from JSON
                data = self._last_detail_json
                cnpj = str(data.get("id") or "").strip()

                inscriptions: List[str] = []
                try:
                # traverse naturezas[*].debitos[*].numero
                    if "naturezas" in data and isinstance(data["naturezas"], list):
                        for natureza in data["naturezas"]:
                            debitos = natureza.get("debitos") or []
                            for deb in debitos:
                                numero = deb.get("numero")
                                if numero:
                                    inscriptions.append(str(numero).strip())
                except Exception as e:
                    logger.error("[DETAIL] Failed extracting inscriptions for %s: %s", cnpj, e)

                debtors.append(DebtorRow(
                    cnpj=cnpj,
                    inscriptions=inscriptions,
                ))
                logger.info("[ROW] %s | inscriptions: %s", cnpj, inscriptions)

                # Close modal
                close_btn = await p.query_selector("button.close, .modal-dialog button.btn")
                if close_btn:
                    await close_btn.click()
                    await p.wait_for_timeout(1000)
            except Exception as row_err:
                logger.error("[ROW] Error handling row %s: %s", idx, row_err)

        return debtors