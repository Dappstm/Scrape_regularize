# pgfn_client.py
from __future__ import annotations
import os
import json
import re
import logging
import unicodedata
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import aiohttp
from playwright.async_api import BrowserContext, Page
from config import PGFN_BASE, PGFN_JSON_HINTS, WAIT_LONG

logger = logging.getLogger("PGFNClient")


@dataclass
class DebtorRow:
    cnpj: str
    company_name: str
    total: Optional[float] = None


@dataclass
class InscriptionRow:
    cnpj: str
    company_name: str
    inscription_number: str
    category: Optional[str]
    amount: Optional[float]


def _matches_json_hint(url: str) -> bool:
    return any(h in url for h in PGFN_JSON_HINTS)


def _to_float_safe(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None


def _normalize_query(s: str) -> str:
    # remove diacritics and common suffixes like "SA"
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\bSA\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


class PGFNClient:
    def __init__(self, context: BrowserContext):
        self.context = context
        self.page: Optional[Page] = None
        self._captured_json: List[Dict[str, Any]] = []
        self._recent_requests: List[Dict[str, Any]] = []
        self._passed_hcaptcha: bool = False
        self._brightdata_token = os.getenv("BRIGHTDATA_API_TOKEN")  # set in env

    async def _brightdata_unblock(self, target_url: str, method: str = "GET", body: Optional[Any] = None, headers: Optional[Dict[str, str]] = None) -> Optional[Any]:
        """
        Use BrightData Web Unlocker to fetch `target_url` with solve_captcha enabled.
        Returns parsed JSON if possible, else raw text.
        """
        api_token = self._brightdata_token
        if not api_token:
            logger.error("BrightData token not set (BRIGHTDATA_API_TOKEN). Cannot use web-unlocker fallback.")
            return None

        unlocker_url = "https://api.brightdata.com/request"
        payload = {
            "zone": "web_unlocker1",
            "url": "https://www.listadevedores.pgfn.gov.br",
            "format": "raw",
            "method": "GET",
            "direct": True
        }
        if body is not None:
            payload["body"] = body
        if headers:
            payload["headers"] = headers

        logger.info("[UNLOCK] Calling BrightData Web Unlocker for %s", target_url)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(unlocker_url,
                                        headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
                                        json=payload,
                                        timeout=60) as resp:
                    text = await resp.text()
                    ct = resp.headers.get("content-type", "")
                    if resp.status != 200:
                        logger.error("[UNLOCK] BrightData returned %s: %s", resp.status, text)
                        return None
                    if "application/json" in ct:
                        try:
                            return json.loads(text)
                        except Exception:
                            return text
                    else:
                        return text
        except Exception as e:
            logger.exception("[UNLOCK] BrightData web-unlocker call failed: %s", e)
            return None

    async def open(self):
        """Open PGFN site and attach response/request listeners to capture JSON API calls."""
        self.page = await self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)
        self._captured_json = []
        self._recent_requests = []

        async def on_response(resp):
            try:
                url = resp.url
                ctype = resp.headers.get("content-type", "")
                status = resp.status
                # store responses that match hints as JSON if possible
                if (("application/json" in ctype) or url.endswith(".json")) and _matches_json_hint(url):
                    try:
                        data = await resp.json()
                        self._captured_json.append({"url": url, "json": data, "status": status})
                        if not self._passed_hcaptcha:
                            # first relevant JSON likely means the site is accessible
                            self._passed_hcaptcha = True
                            logger.info("✅ Captcha bypass confirmed — PGFN JSON API reachable.")
                        logger.info("[XHR] Captured JSON from %s (status=%s keys=%s)", url, status, (list(data.keys()) if isinstance(data, dict) else type(data)))
                    except Exception as e:
                        logger.warning("[XHR] Failed to parse JSON from %s: %s", url, e)
            except Exception as e:
                logger.debug("[XHR] Response hook error: %s", e)

        async def on_request(req):
            try:
                # capture basic request info for debugging/fallback
                info = {"url": req.url, "method": req.method, "resource_type": req.resource_type}
                try:
                    info["post_data"] = req.post_data
                except Exception:
                    info["post_data"] = None
                self._recent_requests.append(info)
                # keep list short
                if len(self._recent_requests) > 80:
                    self._recent_requests.pop(0)
                if req.resource_type == "xhr" or req.url.endswith(".json") or "/api/" in req.url:
                    logger.debug("[REQ] %s %s (post_data=%s)", req.method, req.url, (req.post_data() if hasattr(req, "post_data") else None))
            except Exception:
                pass

        self.page.on("response", on_response)
        self.page.on("request", on_request)

        logger.info("[PGFN] Opening base page: %s", PGFN_BASE)
        await self.page.goto(PGFN_BASE, wait_until="domcontentloaded")

        # Basic captcha presence check
        content = await self.page.content()
        if "captcha" in content.lower() or "hcaptcha" in content.lower():
            logger.warning("⚠️ Page shows captcha/ hcaptcha widget in DOM. The captcha may still need solving.")
        else:
            logger.info("✅ Base page loaded without visible captcha widget in DOM.")

    async def search_company(self, name_query: str) -> List[DebtorRow]:
        """Perform a company search by name and capture debtor rows from JSON responses.
        If the browser XHR is blocked by captcha, fallback to BrightData Web Unlocker for the detected API call.
        """
        assert self.page is not None
        p = self.page
        query = _normalize_query(name_query)
        logger.info("[SEARCH] Normalized query: %s -> %s", name_query, query)

        # clear recent captures
        self._captured_json.clear()
        self._recent_requests.clear()

        # Fill input
        try:
            await p.wait_for_selector("input[placeholder*='Nome'], input[formcontrolname='nome'], input[type='text']", timeout=5000)
            await p.fill("input[placeholder*='Nome'], input[formcontrolname='nome'], input[type='text']", query)
            logger.info("[SEARCH] Filled query into input")
        except Exception:
            logger.warning("[SEARCH] Could not find or fill name input; typing instead")
            await p.keyboard.type(query)

        # Trigger search - try button click first
        clicked = False
        for sel in ["button:has-text('Consultar')", "text=Consultar", "button[type='submit']"]:
            try:
                if await p.locator(sel).count() > 0:
                    await p.click(sel)
                    logger.info("[SEARCH] Clicked %s", sel)
                    clicked = True
                    break
            except Exception as e:
                logger.debug("[SEARCH] click candidate failed (%s): %s", sel, e)
        if not clicked:
            logger.warning("[SEARCH] Click failed; pressing Enter")
            await p.keyboard.press("Enter")

        # Wait a bit for XHRs to fire
        await p.wait_for_timeout(3000)

        # If we already captured valid JSON matching devedores -> parse it
        debtors: List[DebtorRow] = []
        for item in reversed(self._captured_json[-30:]):
            url = item.get("url", "")
            data = item.get("json")
            if not data:
                continue
            text = json.dumps(data, ensure_ascii=False).lower()
            if ("devedor" in text) or ("cnpj" in text and "inscricao" not in text):
                # heuristics: extract arrays
                rows: List[dict] = []
                if isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, list):
                            rows.extend(v)
                elif isinstance(data, list):
                    rows = data
                for r in rows:
                    cnpj = str(r.get("cnpj") or r.get("CNPJ") or r.get("documento") or "").strip()
                    if not cnpj:
                        continue
                    name = (r.get("nome") or r.get("razaoSocial") or r.get("contribuinte") or "").strip()
                    total = _to_float_safe(r.get("total") or r.get("valorTotal") or r.get("montante"))
                    debtors.append(DebtorRow(cnpj=cnpj, company_name=name, total=total))

        if debtors:
            logger.info("✅ Successfully parsed %d debtor rows from captured browser XHRs", len(debtors))
            return debtors

        # --- No debtors captured via normal browser XHRs. Inspect requests we saw to find the candidate API call ---
        logger.warning("[SEARCH] No debtor rows parsed from browser capture; inspecting recent XHRs to find candidate API call.")
        candidate = None
        for req in reversed(self._recent_requests):
            url = req.get("url", "")
            # heuristics: prefer endpoints with 'devedores' or 'consulta' or '/api/' path
            if "devedores" in url or "/api/" in url and ("consulta" in url or "devedores" in url or "buscar" in url):
                candidate = req
                break
            # fallback: any /api/ endpoint
            if candidate is None and "/api/" in url:
                candidate = req

        if not candidate:
            # as last resort, look for any recent xhr that returned json but not collected earlier
            logger.debug("[SEARCH] Recent requests (last 20):")
            for r in self._recent_requests[-20:]:
                logger.debug("  - %s %s", r.get("method"), r.get("url"))
            logger.error("[SEARCH] Could not find a plausible API request to unblock. Please open the site in devtools and copy the search XHR (Copy as cURL) and paste the URL/method into config.")
            return []

        # Candidate request found: try to call it via BrightData Web Unlocker (solve_captcha=True)
        url = candidate.get("url")
        method = candidate.get("method", "GET")
        payload = candidate.get("post_data")
        logger.info("[SEARCH] Candidate API call detected: %s %s (post_data=%s)", method, url, bool(payload))

        # Call BrightData Web Unlocker to fetch the API result with solve_captcha=True
        unlocked = await self._brightdata_unblock(url, method=method, body=payload)
        if not unlocked:
            logger.error("[UNLOCK] BrightData could not unlock the API call or token not provided.")
            return []

        # unlocked may be JSON or text; try to parse rows
        try:
            data = unlocked if isinstance(unlocked, (dict, list)) else (json.loads(unlocked) if isinstance(unlocked, str) else None)
        except Exception:
            data = unlocked

        if not data:
            logger.error("[UNLOCK] No data returned from web-unlocker for %s", url)
            return []

        # Parse the unlocked data similarly to browser-captured data
        rows: List[dict] = []
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    rows.extend(v)
        elif isinstance(data, list):
            rows = data

        for r in rows:
            cnpj = str(r.get("cnpj") or r.get("CNPJ") or r.get("documento") or "").strip()
            if not cnpj:
                continue
            name = (r.get("nome") or r.get("razaoSocial") or r.get("contribuinte") or "").strip()
            total = _to_float_safe(r.get("total") or r.get("valorTotal") or r.get("montante"))
            debtors.append(DebtorRow(cnpj=cnpj, company_name=name, total=total))

        if debtors:
            logger.info("✅ Parsed %d debtor rows from BrightData-unlocked API response", len(debtors))
        else:
            logger.warning("⚠️ BrightData-unlocked response contained no debtor rows for query '%s'", name_query)
            logger.debug("Unlocked response (snippet): %s", str(rows)[:1000])

        return debtors

    async def open_details_and_collect_inscriptions(self, max_entries: Optional[int] = None) -> List[InscriptionRow]:
        """Click Detalhar buttons and capture inscription rows from JSON responses."""
        assert self.page is not None
        p = self.page
        results: List[InscriptionRow] = []

        detail_locators = p.locator("text=Detalhar")
        count = await detail_locators.count()
        logger.info("[DETAIL] Found %d 'Detalhar' buttons", count)
        limit = count if max_entries is None else min(count, max_entries)

        for i in range(limit):
            try:
                await detail_locators.nth(i).click()
                logger.info("[DETAIL] Clicked Detalhar #%d", i)
                await p.wait_for_timeout(1500)

                for item in reversed(self._captured_json[-30:]):
                    data = item.get("json")
                    if not data:
                        continue
                    payload = json.dumps(data, ensure_ascii=False).lower()
                    if "inscricao" in payload or ("inscr" in payload and "cnpj" in payload):
                        def walk_sync(obj):
                            if isinstance(obj, dict):
                                if any(k.lower().startswith("inscr") for k in obj.keys()):
                                    yield obj
                                for v in obj.values():
                                    yield from walk_sync(v)
                            elif isinstance(obj, list):
                                for x in obj:
                                    yield from walk_sync(x)

                        for r in walk_sync(data):
                            results.append(InscriptionRow(
                                cnpj=str(r.get("cnpj") or "").strip(),
                                company_name=str(r.get("nome") or r.get("razaoSocial") or "").strip(),
                                inscription_number=str(r.get("inscricao") or r.get("numero") or "").strip(),
                                category=r.get("categoria") or r.get("natureza"),
                                amount=_to_float_safe(r.get("valor") or r.get("montante") or r.get("total")),
                            ))
                            logger.debug("[DETAIL] Captured inscription: %s", r)
            except Exception as e:
                logger.warning("[DETAIL] Failed to click Detalhar #%d: %s", i, e)

        uniq = {(r.cnpj, r.inscription_number): r for r in results}
        logger.info("[DETAIL] Collected %d unique inscriptions", len(uniq))
        return list(uniq.values())