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
        self._passed_hcaptcha: bool = False  # set in main.py once token injected
        self._captured_json = []


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
                # Log every response for debugging
                logger.debug("[XHR] %s %s (status=%s, content-type=%s)", 
                            resp.request.method, resp.url, resp.status, resp.headers.get("content-type", ""))

                # Check if response is a POST to /api/devedores/
                if resp.request.method == "POST" and "devedores" in resp.url.lower():
                    logger.info("[XHR] Captured POST to %s (status=%s)", resp.url, resp.status)
                
                # Log headers for debugging (e.g., Recaptcha)
                    logger.debug("[XHR] Request headers: %s", resp.request.headers)

                # Check content-type and URL for JSON
                    ctype = resp.headers.get("content-type", "").lower()
                    if "application/json" in ctype or resp.url.endswith(".json"):
                        try:
                            data = await resp.json()
                            self._captured_json.append({"url": resp.url, "json": data, "status": resp.status})
                            logger.info(
                                "[XHR] Captured JSON from %s (status=%s, keys=%s)",
                                resp.url, resp.status, list(data.keys()) if isinstance(data, dict) else type(data)
                            )
                        except Exception as e:
                            logger.warning("[XHR] Failed to parse JSON from %s: %s", resp.url, e)
                    else:
                        logger.warning("[XHR] Non-JSON response from %s (content-type=%s)", resp.url, ctype)
                elif _matches_json_hint(resp.url):
                    # Capture other JSON responses as fallback
                    ctype = resp.headers.get("content-type", "").lower()
                    if "application/json" in ctype or resp.url.endswith(".json"):
                        try:
                            data = await resp.json()
                            self._captured_json.append({"url": resp.url, "json": data, "status": resp.status})
                            logger.debug(
                                "[XHR] Captured JSON from %s (status=%s, keys=%s)",
                                resp.url, resp.status, list(data.keys()) if isinstance(data, dict) else type(data)
                            )
                        except Exception as e:
                            logger.debug("[XHR] Failed to parse JSON from %s: %s", resp.url, e)
            except Exception as e:
                logger.error("[XHR] Response hook error for %s: %s", resp.url, e)

        self.page.on("response", on_response)
        logger.info("[PGFN] Opening base page: %s", PGFN_BASE)
        await self.page.goto(PGFN_BASE, wait_until="domcontentloaded")
        logger.info("[PGFN] Base page loaded.")

    async def search_company(self, name_query: str, max_retries: int = 2) -> List[DebtorRow]:
        """Perform a company search by calling /api/devedores/ directly."""
        assert self.page is not None
        p = self.page

        # Capture Authorization header (if set by page)
        authorization_token = await p.evaluate("localStorage.getItem('auth_token') || sessionStorage.getItem('auth_token') || ''")
        if authorization_token:
            logging.info("[SEARCH] Found Authorization token: Bearer %s", authorization_token[:10] + "...")

        for attempt in range(max_retries + 1):
            # Clear previous captured JSON
            self._captured_json = []

            # Step 1: Fill form and click CONSULTAR
            await page.wait_for_timeout(5000)  # Wait 5s
            await page.mouse.move(500, 500, steps=10)  # Simulate mouse movement
            await p.mouse.click(200, 200)
            await page.wait_for_timeout(1000)
            await p.fill("input#nome, input[formcontrolname='nome']", name_query)
            await self._bulletproof_click(
                "button:has-text('Consultar'), button.btn.btn-warning",
                "CONSULTAR",
                allow_enter=True,
            )

            # Step 2: Wait for XHR response
            await p.wait_for_timeout(60000)  # Wait 60s
            logger.info("[SEARCH] Checking captured_json for /api/devedores/ response (attempt %s/%s)", attempt + 1, max_retries + 1)

            # Step 3: Find /api/devedores/ response with status 200
            data = None
            for item in self._captured_json:
                if "/api/devedores/" in item["url"].lower() and item.get("status") == 200:
                    logger.info("[SEARCH] Found /api/devedores/ in captured_json: %s (status=%s)", item["url"], item["status"])
                    data = item["json"]
                    logger.debug("[SEARCH] Raw devedores JSON type: %s, keys=%s", type(data), list(data.keys()) if isinstance(data, dict) else [])
                    logger.debug("[SEARCH] Full response JSON: %s", data)
                    break
            else:
                # Log all /api/devedores/ responses
                for item in self._captured_json:
                    if "/api/devedores/" in item["url"].lower():
                        logger.warning("[SEARCH] Found /api/devedores/ with status %s: %s", item["status"], item["json"])
                if attempt < max_retries:
                    logger.info("[SEARCH] Retrying due to no status 200 response...")
                    # Refresh page to reset session
                    await p.goto(PGFN_BASE, wait_until="domcontentloaded")
                    continue
                logger.error("[SEARCH] No /api/devedores/ response with status 200 after %s attempts", max_retries + 1)
                return []

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

                debtor = DebtorRow(
                    cnpj=cnpj,
                    company_name=name,
                    fantasy_name=fantasy,
                    total=total,
                )
                debtors.append(debtor)
                logger.debug("[SEARCH] Parsed debtor row: %s", debtor)

            # Deduplicate
            seen = set()
            unique: List[DebtorRow] = []
            for d in debtors:
                if d.cnpj not in seen:
                    unique.append(d)
                    seen.add(d.cnpj)

            logger.info("✅ Parsed %d debtor rows for query '%s'", len(unique), name_query)
            return unique
    
        logger.error("[SEARCH] Failed to get valid /api/devedores/ response after %s attempts", max_retries + 1)
        return []

    async def collect_inscriptions_from_devedores(
        self, devedores_payload: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> List[InscriptionRow]:
        """
        Fetch and extract inscription rows directly from /api/devedores/{id}.
        This bypasses EXPORTAR and uses the backend API for details.
        """
        results: List[InscriptionRow] = []

        # Normalize records
        if isinstance(devedores_payload, dict):
            records = devedores_payload.get("devedores", [])
        elif isinstance(devedores_payload, list):
            records = devedores_payload
        else:
            logger.warning("[DETAIL] Unexpected devedores payload type: %s", type(devedores_payload))
            return results

        # Grab cookies from Playwright context (same trick as in search_company)
        context_cookies = await self.context.cookies()
        cookies = {c["name"]: c["value"] for c in context_cookies}

        headers = {
           "Accept": "application/json, text/plain, */*",
           "Content-Type": "application/json;charset=UTF-8",
        }

        async with httpx.AsyncClient(cookies=cookies, headers=headers, timeout=30.0) as client:
            for r in records:
                cnpj = str(r.get("id") or "").strip()
                if not cnpj:
                    continue

                try:
                    api_url = f"https://www.listadevedores.pgfn.gov.br/api/devedores/{cnpj}"
                    resp = await client.get(api_url)
                    resp.raise_for_status()
                    detail_data = resp.json()
                    logger.debug("[DETAIL] Got detail for CNPJ %s: keys=%s",
                                 cnpj, list(detail_data.keys()) if isinstance(detail_data, dict) else type(detail_data))

                    row = InscriptionRow(
                        cnpj=cnpj,
                        company_name=str(r.get("nome") or "").strip(),
                        amount=_to_float_safe(r.get("totaldivida"))
                    )
                    results.append(row)
                    logger.debug("[DETAIL] Parsed inscription row: %s", row)
                except Exception as e:
                    logger.error("[DETAIL] Failed to fetch detail for CNPJ %s: %s", cnpj, e)

        logger.info("[DETAIL] Collected %d inscriptions from API", len(results))
        return results