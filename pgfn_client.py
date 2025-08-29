# pgfn_client.py
from __future__ import annotations
import json, time, logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
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
        """Open PGFN site and attach response listeners to capture JSON API calls."""
        self.page = self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)

        def on_response(resp):
            try:
                url = resp.url
                ctype = resp.headers.get("content-type", "")
                if (("application/json" in ctype) or url.endswith(".json")) and _matches_json_hint(url):
                    try:
                        data = resp.json()
                        self._captured_json.append({"url": url, "json": data})
                        logger.debug("Captured JSON XHR: %s", url)
                    except Exception:
                        pass
            except Exception:
                pass

        self.page.on("response", on_response)
        self.page.goto(PGFN_BASE, wait_until="domcontentloaded")

    def search_company(self, name_query: str) -> List[DebtorRow]:
        """Perform a company search by name and capture debtor rows from JSON responses."""
        assert self.page is not None
        p = self.page

        # Try multiple selectors for the input
        sel_candidates = [
            "input[placeholder*='Nome']",
            "input[formcontrolname='nome']",
            "input[type='text']",
        ]
        for sel in sel_candidates:
            try:
                if p.locator(sel).count() > 0:
                    p.fill(sel, name_query)
                    break
            except Exception:
                continue
        else:
            # fallback: type directly
            p.keyboard.type(name_query)

        # Click "Consultar"
        for b in ["button:has-text('Consultar')", "text=Consultar", "button[type='submit']"]:
            try:
                if p.locator(b).count() > 0:
                    p.click(b)
                    break
            except Exception:
                continue
        else:
            p.keyboard.press("Enter")

        # wait for XHRs
        p.wait_for_timeout(2000)

        # Parse recent captured JSONs
        debtors: List[DebtorRow] = []
        for item in reversed(self._captured_json[-20:]):
            data = item.get("json")
            if not data:
                continue
            text = json.dumps(data, ensure_ascii=False).lower()
            if "devedor" in text or ("cnpj" in text and "inscricao" not in text):
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

        # dedupe
        seen, unique = set(), []
        for d in debtors:
            if d.cnpj not in seen:
                unique.append(d)
                seen.add(d.cnpj)
        return unique

    def open_details_and_collect_inscriptions(self, max_entries: Optional[int] = None) -> List[InscriptionRow]:
        """Click Detalhar buttons and capture inscription rows from JSON responses."""
        assert self.page is not None
        p = self.page
        results: List[InscriptionRow] = []

        detail_locators = p.locator("text=Detalhar")
        count = detail_locators.count()
        limit = count if max_entries is None else min(count, max_entries)

        for i in range(limit):
            try:
                detail_locators.nth(i).click()
                p.wait_for_timeout(1200)

                for item in reversed(self._captured_json[-30:]):
                    data = item.get("json")
                    if not data:
                        continue
                    payload = json.dumps(data, ensure_ascii=False).lower()
                    if "inscricao" in payload or ("inscr" in payload and "cnpj" in payload):
                        def walk(obj):
                            if isinstance(obj, dict):
                                if any(k.lower().startswith("inscr") for k in obj.keys()):
                                    yield obj
                                for v in obj.values():
                                    yield from walk(v)
                            elif isinstance(obj, list):
                                for x in obj:
                                    yield from walk(x)
                        for r in walk(data):
                            results.append(InscriptionRow(
                                cnpj=str(r.get("cnpj") or "").strip(),
                                company_name=str(r.get("nome") or r.get("razaoSocial") or "").strip(),
                                inscription_number=str(r.get("inscricao") or r.get("numero") or "").strip(),
                                category=r.get("categoria") or r.get("natureza"),
                                amount=_to_float_safe(r.get("valor") or r.get("montante") or r.get("total")),
                            ))
            except Exception:
                continue

        uniq = {(r.cnpj, r.inscription_number): r for r in results}
        return list(uniq.values())