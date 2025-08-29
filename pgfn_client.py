# pgfn_client_async.py (patched)

from __future__ import annotations
import json, logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
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
        s = s.replace('.', '').replace(',', '.')
        return float(s)
    except Exception:
        return None


class PGFNClient:
    def __init__(self, context: BrowserContext):
        self.context = context
        self.page: Optional[Page] = None
        self._captured_json: List[Dict[str, Any]] = []
        self._passed_hcaptcha: bool = False

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
                        if not self._passed_hcaptcha:
                            self._passed_hcaptcha = True
                            logger.info("✅ BrightData handled hCaptcha — PGFN JSON API is accessible.")
                        logger.info("[XHR] Captured JSON from %s (keys=%s)", url, list(data.keys()) if isinstance(data, dict) else type(data))
                    except Exception as e:
                        logger.warning("[XHR] Failed to parse JSON from %s: %s", url, e)
            except Exception as e:
                logger.debug("[XHR] Response hook error: %s", e)

        self.page.on("response", on_response)
        logger.info("[PGFN] Opening base page: %s", PGFN_BASE)
        await self.page.goto(PGFN_BASE, wait_until="domcontentloaded")

        # Check if still stuck at challenge
        content = await self.page.content()
        if "captcha" in content.lower() or "hcaptcha" in content.lower():
            logger.error("❌ Still seeing a captcha challenge — BrightData session may not be configured correctly.")
        else:
            logger.info("✅ Base page loaded without visible captcha.")

    async def search_company(self, name_query: str) -> List[DebtorRow]:
        """Perform a company search by name and capture debtor rows from JSON responses."""
        assert self.page is not None
        p = self.page

        try:
            await p.wait_for_selector("input[placeholder*='Nome'], input[formcontrolname='nome'], input[type='text']", timeout=5000)
        except Exception:
            logger.warning("[SEARCH] Could not find name input field!")
        else:
            await p.fill("input[placeholder*='Nome'], input[formcontrolname='nome'], input[type='text']", name_query)
            logger.info("[SEARCH] Filled search with: %s", name_query)

        try:
            await p.click("button:has-text('Consultar'), text=Consultar, button[type='submit']")
            logger.info("[SEARCH] Clicked Consultar button")
        except Exception:
            logger.warning("[SEARCH] Failed to click Consultar — falling back to Enter key")
            await p.keyboard.press("Enter")

        await p.wait_for_timeout(4000)

        if not self._captured_json:
            logger.warning("[SEARCH] No JSON captured yet — maybe endpoint differs or captcha blocked request?")
        else:
            logger.info("[SEARCH] Captured %d JSON responses", len(self._captured_json))
            for item in self._captured_json[-5:]:
                logger.debug("[SEARCH] Captured JSON from: %s", item["url"])

        debtors: List[DebtorRow] = []
        for item in reversed(self._captured_json[-20:]):
            data = item.get("json")
            if not data:
                continue
            text = json.dumps(data, ensure_ascii=False).lower()
            if ("devedor" in text) or ("cnpj" in text and "inscricao" not in text):
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

        seen = set()
        unique = []
        for d in debtors:
            if d.cnpj not in seen:
                unique.append(d)
                seen.add(d.cnpj)

        if unique:
            logger.info("✅ Successfully parsed %d debtor rows for query '%s'", len(unique), name_query)
        else:
            logger.warning("⚠️ No debtor rows parsed for '%s'", name_query)

        return unique

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