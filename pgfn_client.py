# pgfn_client.py
from __future__ import annotations
import logging, random
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from playwright.async_api import BrowserContext, Page
from config import PGFN_BASE, PGFN_JSON_HINTS, WAIT_LONG

logger = logging.getLogger("PGFNClient")


@dataclass
class DebtorRow:
    cnpj: str
    company_name: str
    fantasy_name: Optional[str] = None
    total: Optional[float] = None


def _matches_json_hint(url: str) -> bool:
    return any(h in url for h in PGFN_JSON_HINTS)


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
        self._captured_json: List[Dict[str, Any]] = []

    async def _bulletproof_click(self, selector: str, label: str, allow_enter: bool = False) -> bool:
        """Try multiple strategies to click a button reliably."""
        assert self.page is not None
        p = self.page
        try:
            await p.click(selector)
            logger.info("[CLICK] Clicked %s (normal)", label)
            return True
        except Exception as e1:
            logger.warning("[CLICK] Normal click failed on %s: %s", label, e1)
            try:
                await p.click(selector, force=True)
                logger.info("[CLICK] Clicked %s (force)", label)
                return True
            except Exception as e2:
                logger.warning("[CLICK] Force click failed on %s: %s", label, e2)
                try:
                    await p.locator(selector).evaluate("btn => btn.click()")
                    logger.info("[CLICK] Triggered %s via JS dispatch", label)
                    return True
                except Exception as e3:
                    logger.error("[CLICK] JS dispatch failed on %s: %s", label, e3)
                    if allow_enter:
                        await p.keyboard.press("Enter")
                        logger.info("[CLICK] Pressed Enter as fallback for %s", label)
                        return True
        return False

    async def open(self):
        """Open PGFN site and attach response listeners to capture JSON API calls."""
        self.page = await self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)

        async def on_response(resp):
            try:
                if resp.request.method == "POST" and "devedores" in resp.url.lower():
                    ctype = resp.headers.get("content-type", "").lower()
                    if "application/json" in ctype:
                        try:
                            data = await resp.json()
                            self._captured_json.append(
                                {"url": resp.url, "json": data, "status": resp.status}
                            )
                            logger.info("[XHR] Captured JSON from %s (status=%s)", resp.url, resp.status)
                        except Exception as e:
                            logger.warning("[XHR] Failed to parse JSON: %s", e)
                elif _matches_json_hint(resp.url):
                    ctype = resp.headers.get("content-type", "").lower()
                    if "application/json" in ctype:
                        try:
                            data = await resp.json()
                            self._captured_json.append(
                                {"url": resp.url, "json": data, "status": resp.status}
                            )
                        except Exception:
                            pass
            except Exception as e:
                logger.error("[XHR] Response hook error for %s: %s", resp.url, e)

        self.page.on("response", on_response)
        logger.info("[PGFN] Opening base page: %s", PGFN_BASE)
        await self.page.goto(PGFN_BASE, wait_until="domcontentloaded")
        logger.info("[PGFN] Base page loaded.")

    async def search_company(self, name_query: str, max_retries: int = 2) -> List[DebtorRow]:
        """Perform a company search by simulating UI and collecting /api/devedores/ response."""
        assert self.page is not None
        p = self.page

        for attempt in range(max_retries + 1):
            self._captured_json = []

            # Fill search form
            await p.wait_for_timeout(random.randint(1000, 2000))
            await p.fill("input#nome, input[formcontrolname='nome']", name_query)
            await p.wait_for_timeout(random.randint(1000, 2000))

            await self._bulletproof_click(
                "button:has-text('Consultar'), button.btn.btn-warning",
                "CONSULTAR",
                allow_enter=True,
            )

            # Wait for XHR to arrive
            await p.wait_for_timeout(60000)
            logger.info("[SEARCH] Checking captured_json for /api/devedores/ response (attempt %s/%s)", attempt + 1, max_retries + 1)

            data = None
            for item in self._captured_json:
                if "/api/devedores/" in item["url"].lower() and item.get("status") == 200:
                    data = item["json"]
                    break

            if not data:
                if attempt < max_retries:
                    logger.info("[SEARCH] Retrying (no valid response)...")
                    await p.goto(PGFN_BASE, wait_until="domcontentloaded")
                    continue
                logger.error("[SEARCH] No valid /api/devedores/ response after retries")
                return []

            # Parse debtor rows
            records = []
            if isinstance(data, dict) and "devedores" in data:
                records = data["devedores"]
            elif isinstance(data, list):
                records = data

            debtors: List[DebtorRow] = []
            for r in records:
                cnpj = str(r.get("id") or "").strip()
                if not cnpj:
                    continue
                debtor = DebtorRow(
                    cnpj=cnpj,
                    company_name=str(r.get("nome") or "").strip(),
                    fantasy_name=str(r.get("nomefantasia") or "").strip(),
                    total=_to_float_safe(r.get("totaldivida")),
                )
                debtors.append(debtor)

            # Deduplicate
            seen = set()
            unique = [d for d in debtors if not (d.cnpj in seen or seen.add(d.cnpj))]
            logger.info("✅ Parsed %d debtor rows for query '%s'", len(unique), name_query)
            return unique

        return []