# pgfn_client_async.py (refactored: open() no longer does captcha detection/solving)
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
        self._passed_hcaptcha: bool = False  # set in main.py once token injected

    async def _bulletproof_click(self, selector: str, label: str, allow_enter: bool = False) -> bool:
        """Try multiple strategies to click a button."""
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
                    await p.evaluate(
                        """(sel) => {
                            const btn = document.querySelector(sel);
                            if (btn) btn.click();
                        }""",
                        selector,
                    )
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

    # search_company() and open_details_and_collect_inscriptions() remain unchanged

    async def search_company(self, name_query: str) -> List[DebtorRow]:
        """Perform a company search by name and capture debtor rows from JSON responses."""
        assert self.page is not None
        p = self.page

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

        await self._bulletproof_click(
            "button:has-text('Consultar'), text=Consultar, button[type='submit']",
            "Consultar",
            allow_enter=True,
        )

        await p.wait_for_timeout(4000)

        if not self._captured_json:
            logger.warning("[SEARCH] No JSON captured yet — maybe endpoint differs or captcha blocked request?")
        else:
            logger.info("[SEARCH] Captured %d JSON responses", len(self._captured_json))
            for item in self._captured_json[-5:]:
                logger.debug("[SEARCH] Captured JSON from: %s", item["url"])

        debtors: List[DebtorRow] = []

        logger.debug("[SEARCH] Inspecting last %d JSON responses", min(20, len(self._captured_json)))
        for idx, item in enumerate(reversed(self._captured_json[-20:])):
            data = item.get("json")
            url = item.get("url")
            if not data:
                logger.debug("[SEARCH][%d] Skipping empty JSON from %s", idx, url)
                continue

            # Log structure of JSON
            if isinstance(data, dict):
                logger.debug("[SEARCH][%d] JSON dict keys from %s: %s", idx, url, list(data.keys()))
            elif isinstance(data, list):
                logger.debug("[SEARCH][%d] JSON list length %d from %s", idx, len(data), url)
            else:
                logger.debug("[SEARCH][%d] Unexpected JSON type %s from %s", idx, type(data), url)
                continue

            text = json.dumps(data, ensure_ascii=False).lower()
            if ("devedor" in text) or ("cnpj" in text and "inscricao" not in text):
                logger.debug("[SEARCH][%d] Candidate JSON contains debtor-like fields", idx)
                rows: List[dict] = []

                if isinstance(data, dict):
                    for k, v in data.items():
                        if isinstance(v, list):
                            logger.debug("[SEARCH][%d] Found list under key '%s' with %d rows", idx, k, len(v))
                            rows.extend(v)
                elif isinstance(data, list):
                    logger.debug("[SEARCH][%d] Treating JSON as list with %d rows", idx, len(data))
                    rows = data

                for r_idx, r in enumerate(rows):
                    cnpj = str(r.get("cnpj") or r.get("CNPJ") or r.get("documento") or "").strip()
                    if not cnpj:
                        logger.debug("[SEARCH][%d][row %d] Skipping row with no CNPJ: %s", idx, r_idx, r)
                        continue

                    name = (r.get("nome") or r.get("razaoSocial") or r.get("contribuinte") or "").strip()
                    total = _to_float_safe(r.get("total") or r.get("valorTotal") or r.get("montante"))

                    logger.debug(
                        "[SEARCH][%d][row %d] Parsed debtor row: cnpj=%s, name='%s', total=%s",
                        idx, r_idx, cnpj, name, total
                    )

                    debtors.append(DebtorRow(cnpj=cnpj, company_name=name, total=total))
            else:
                logger.debug("[SEARCH][%d] JSON does not match debtor pattern (keys=%s)", idx, list(data)[:10])

        # Deduplicate by CNPJ
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
            # Extra dump for investigation
            logger.debug("[SEARCH] Last JSON payload (truncated): %s", json.dumps(self._captured_json[-1].get("json"), ensure_ascii=False)[:500])

        return unique

    async def open_details_and_collect_inscriptions(
        self, max_entries: Optional[int] = None
    ) -> List[InscriptionRow]:
        """Click Detalhar buttons and capture inscription rows from JSON responses."""
        assert self.page is not None
        p = self.page
        results: List[InscriptionRow] = []

        detail_locators = p.locator("text=Detalhar")
        count = await detail_locators.count()
        logger.info("[DETAIL] Found %d 'Detalhar' buttons", count)
        limit = count if max_entries is None else min(count, max_entries)

        for i in range(limit):
            clicked = await self._bulletproof_click(
                f"(//button[contains(., 'Detalhar')])[{i+1}]",
                f"Detalhar #{i}",
                allow_enter=False,
            )
            if not clicked:
                logger.warning("[DETAIL] Could not click Detalhar #%d", i)
                continue

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
                        results.append(
                            InscriptionRow(
                                cnpj=str(r.get("cnpj") or "").strip(),
                                company_name=str(r.get("nome") or r.get("razaoSocial") or "").strip(),
                                inscription_number=str(r.get("inscricao") or r.get("numero") or "").strip(),
                                category=r.get("categoria") or r.get("natureza"),
                                amount=_to_float_safe(r.get("valor") or r.get("montante") or r.get("total")),
                            )
                        )
                        logger.debug("[DETAIL] Captured inscription: %s", r)

        uniq = {(r.cnpj, r.inscription_number): r for r in results}
        logger.info("[DETAIL] Collected %d unique inscriptions", len(uniq))
        return list(uniq.values())