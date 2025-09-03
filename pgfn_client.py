# pgfn_client.py
from __future__ import annotations
import json, logging
from typing import List, Dict, Any, Optional, Union
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


@dataclass
class InscriptionRow:
    cnpj: str
    company_name: str
    amount: Optional[float] = None


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
        self._passed_hcaptcha: bool = False  # set in main.py once token injected

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
                url = resp.url
                ctype = resp.headers.get("content-type", "")
                if (("application/json" in ctype) or url.endswith(".json")) and _matches_json_hint(url):
                    try:
                        data = await resp.json()
                        self._captured_json.append({"url": url, "json": data})
                        logger.info(
                            "[XHR] Captured JSON from %s (keys=%s)",
                            url,
                            list(data.keys()) if isinstance(data, dict) else type(data),
                        )
                    except Exception as e:
                        logger.warning("[XHR] Failed to parse JSON from %s: %s", url, e)
            except Exception as e:
                logger.debug("[XHR] Response hook error: %s", e)

        self.page.on("response", on_response)
        logger.info("[PGFN] Opening base page: %s", PGFN_BASE)
        await self.page.goto(PGFN_BASE, wait_until="domcontentloaded")
        logger.info("[PGFN] Base page loaded.")

    async def search_company(self, name_query: str) -> List[DebtorRow]:
        """Perform a company search by name and capture debtor rows from devedores/ JSON."""
        assert self.page is not None
        p = self.page

        # Fill search field
        try:
            await p.wait_for_selector(
                "input[placeholder*='Nome'], input[formcontrolname='nome'], input[type='text']", 
                timeout=5000
            )
        except Exception:
            logger.warning("[SEARCH] Could not find name input field!")
        else:
            await p.fill(
                "input[placeholder*='Nome'], input[formcontrolname='nome'], input[type='text']",
                name_query,
            )
            logger.info("[SEARCH] Filled search with: %s", name_query)

        # Click "CONSULTAR"
        await self._bulletproof_click(
            "button:has-text('Consultar'), button.btn.btn-warning",
            "CONSULTAR",
            allow_enter=True,
        )

        # Wait a bit for the XHR to fire
        # await p.wait_for_timeout(5000)
        
        # Wait for CONSULTAR results to load
        try:
            await p.wait_for_response(
                lambda r: "devedores/" in r.url and r.request.method == "POST",
                timeout=30000,
            )
            logger.info("[SEARCH] devedores/ response captured after CONSULTAR")
        except Exception:
            logger.warning("[SEARCH] No devedores/ response detected after CONSULTAR click")

        # Filter captured JSON for devedores/
        devedores_payloads = [
            item for item in self._captured_json 
            if "devedores" in item.get("url", "")
        ]

        if not devedores_payloads:
            logger.warning("[SEARCH] No devedores/ JSON found after query '%s'", name_query)
            return []

        # Use the latest devedores/ response
        latest = devedores_payloads[-1]
        data = latest.get("json") or {}
        logger.debug("[SEARCH] Raw devedores JSON type: %s", type(data))

        debtors: List[DebtorRow] = []

        # Case 1: {"pagina": 1, "devedores": [...]}
        if isinstance(data, dict) and "devedores" in data:
            records = data["devedores"]
        # Case 2: plain list
        elif isinstance(data, list):
            records = data
        else:
            records = []

        for r in records:
            cnpj = str(r.get("id") or "").strip()
            if not cnpj:
                continue
            name = str(r.get("nome") or "").strip()
            fantasy = str(r.get("nomefantasia") or "").strip()
            total = _to_float_safe(r.get("totaldivida"))

            debtors.append(
                DebtorRow(
                    cnpj=cnpj,
                    company_name=name,
                    fantasy_name=fantasy,
                    total=total,
                )
            )
            logger.debug("[SEARCH] Parsed debtor row: %s", debtors[-1])

        # Deduplicate
        seen = set()
        unique = []
        for d in debtors:
            if d.cnpj not in seen:
                unique.append(d)
                seen.add(d.cnpj)

        logger.info("✅ Parsed %d debtor rows for query '%s'", len(unique), name_query)
        return unique

    async def collect_inscriptions_from_devedores(
        self, devedores_payload: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> List[InscriptionRow]:
        """Extract inscription rows directly from devedores JSON (no EXPORTAR click needed)."""
        results: List[InscriptionRow] = []

        if isinstance(devedores_payload, dict):
            records = devedores_payload.get("devedores", [])
        elif isinstance(devedores_payload, list):
            records = devedores_payload
        else:
            logger.warning("[DETAIL] Unexpected devedores payload type: %s", type(devedores_payload))
            return results

        for r in records:
            cnpj = str(r.get("id") or "").strip()
            if not cnpj:
                continue
            row = InscriptionRow(
                cnpj=cnpj,
                company_name=str(r.get("nome") or "").strip(),
                amount=_to_float_safe(r.get("totaldivida")),
            )
            results.append(row)
            logger.debug("[DETAIL] Parsed inscription row: %s", row)

        logger.info("[DETAIL] Collected %d inscriptions directly from devedores JSON", len(results))
        return results