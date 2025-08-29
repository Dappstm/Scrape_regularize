# pgfn_client.py
from __future__ import annotations
import json, time, logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
from playwright.sync_api import BrowserContext, Page
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

    def open(self):
        self.page = self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)
        # attach listener to capture JSON XHRs
        def on_response(resp):
            try:
                url = resp.url
                ctype = resp.headers.get("content-type", "")
                if (("application/json" in ctype) or url.endswith(".json")) and _matches_json_hint(url):
                    try:
                        text = resp.text()
                        data = json.loads(text)
                        self._captured_json.append({"url": url, "json": data})
                        logger.debug("Captured JSON XHR: %s", url)
                    except Exception:
                        # fallback: ignore parse errors
                        pass
            except Exception:
                pass
        self.page.on("response", on_response)
        self.page.goto(PGFN_BASE, wait_until="domcontentloaded")

    def _wait_for_user_to_solve_captcha(self, timeout_sec: int = 120):
        # heuristic: wait until user interacts or a known element appears
        # Caller may solve captcha manually in the opened page.
        logger.info("If hCaptcha appears, please solve it in the opened browser window.")
        # simple sleep; you can replace with better detection if desired
        time.sleep(2)

    def search_company(self, name_query: str) -> List[DebtorRow]:
        assert self.page is not None
        p = self.page
        # try robust fill by multiple selectors
        sel_candidates = [
            "input[placeholder*='Nome']",
            "input[formcontrolname='nome']",
            "input[type='text']"
        ]
        filled = False
        for sel in sel_candidates:
            try:
                if p.locator(sel).count() > 0:
                    p.fill(sel, name_query)
                    filled = True
                    break
            except Exception:
                continue
        if not filled:
            # fallback: focus body and type
            p.keyboard.type(name_query)

        # click Consultar
        btn_candidates = ["button:has-text('Consultar')", "text=Consultar", "button[type='submit']"]
        clicked = False
        for b in btn_candidates:
            try:
                if p.locator(b).count() > 0:
                    p.click(b)
                    clicked = True
                    break
            except Exception:
                continue
        if not clicked:
            p.keyboard.press("Enter")

        # wait a bit for XHRs to fire
        p.wait_for_timeout(2000)

        # parse recent captured JSONs for debtor rows
        debtors: List[DebtorRow] = []
        for item in reversed(self._captured_json[-20:]):
            data = item.get("json")
            if not data:
                continue
            text = json.dumps(data, ensure_ascii=False).lower()
            if ("devedor" in text) or ("cnpj" in text and "inscricao" not in text):
                # try find array lists inside data
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

        # fallback: try to read rows from DOM table if JSON failed (defensive)
        if not debtors:
            try:
                rows = p.locator("table tr")
                for i in range(rows.count()):
                    text = rows.nth(i).inner_text()
                    # simple heuristic parser, not perfect
                    if "cnpj" in text.lower() or len(text.split()) > 2:
                        # skip header row heuristically
                        pass
            except Exception:
                pass

        # dedupe by CNPJ
        seen = set()
        unique = []
        for d in debtors:
            if d.cnpj not in seen:
                unique.append(d)
                seen.add(d.cnpj)
        return unique

    def open_details_and_collect_inscriptions(self, max_entries: Optional[int] = None) -> List[InscriptionRow]:
        assert self.page is not None
        p = self.page
        results: List[InscriptionRow] = []

        # Click each 'Detalhar' button present on the page to trigger the detail XHRs
        detail_locators = p.locator("text=Detalhar")
        count = detail_locators.count() if detail_locators else 0
        limit = count if max_entries is None else min(count, max_entries)

        for i in range(limit):
            try:
                detail_locators.nth(i).click()
                p.wait_for_timeout(1200)
                # scan recent captured JSONs for inscriptions
                for item in reversed(self._captured_json[-30:]):
                    data = item.get("json")
                    if not data:
                        continue
                    payload = json.dumps(data, ensure_ascii=False).lower()
                    # look for patterns that indicate inscriptions arrays
                    if ("inscricao" in payload) or ("inscr" in payload and "cnpj" in payload):
                        # flatten any nested lists/dicts to find objects that have 'inscricao' and 'cnpj'
                        def walk(obj):
                            if isinstance(obj, dict):
                                if any(k.lower().startswith("inscr") or k.lower().startswith("numero") for k in obj.keys()):
                                    yield obj
                                for v in obj.values():
                                    yield from walk(v)
                            elif isinstance(obj, list):
                                for x in obj:
                                    yield from walk(x)
                        rows = list(walk(data))
                        for r in rows:
                            cnpj = str(r.get("cnpj") or r.get("CNPJ") or "").strip()
                            company = str(r.get("nome") or r.get("razaoSocial") or "").strip()
                            insc = str(r.get("inscricao") or r.get("inscricaoNumero") or r.get("numero") or "").strip()
                            cat = r.get("categoria") or r.get("natureza") or None
                            amt = r.get("valor") or r.get("montante") or r.get("total")
                            results.append(InscriptionRow(cnpj=cnpj or "", company_name=company or "",
                                                         inscription_number=insc or "", category=(cat or None),
                                                         amount=_to_float_safe(amt)))
            except Exception:
                continue

        # dedupe by (cnpj, inscription_number)
        uniq = {}
        for r in results:
            key = (r.cnpj, r.inscription_number)
            if key not in uniq:
                uniq[key] = r
        return list(uniq.values())