from __future__ import annotations
import json, re, time
from typing import List, Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass
from playwright.sync_api import sync_playwright, Page, BrowserContext
from config import PGFN_BASE, PGFN_JSON_HINTS, WAIT_LONG, WAIT_MED, WAIT_SHORT

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
    if isinstance(val, (int,float)):
        return float(val)
    s = str(val)
    # Convert Brazilian formats like "1.234,56" -> 1234.56
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except:
        return None

class PGFNClient:
    def __init__(self, context: BrowserContext):
        self.context = context
        self.page: Optional[Page] = None
        self._captured_json: List[Dict[str, Any]] = []

    def open(self):
        self.page = self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)
        self.page.on("response", self._on_response)
        self.page.goto(PGFN_BASE, wait_until="domcontentloaded")

    def _on_response(self, response):
        try:
            url = response.url
            ctype = response.headers.get("content-type", "").lower()
            if ("application/json" in ctype or url.endswith(".json")) and _matches_json_hint(url):
                body = response.body()
                if body:
                    data = json.loads(body.decode("utf-8", errors="ignore"))
                    self._captured_json.append({"url": url, "json": data})
        except Exception:
            pass  # be resilient

    def wait_for_captcha_solved(self):
        # Heuristic: wait until the main search input is usable and results can be fetched.
        # If an hCaptcha iframe is visible, user must solve it.
        # We simply wait for the results table to be available after a search.
        pass  # explicitly no bypass here

    def search_company(self, name_query: str) -> List[DebtorRow]:
        assert self.page is not None
        # Try to find search input by common selectors; may need adjustment if portal changes:
        # Strategy: type, click Consultar, wait for any JSON results
        # Fallback: we also attempt to read table rows if JSON capture fails.
        p = self.page
        # Focus search input (uses heuristic CSS/XPath that may need tuning)
        # Use multiple selectors for robustness
        selector_candidates = [
            "input[placeholder*=Nome]", 
            "input[formcontrolname='nome']",
            "input[type='text']"
        ]
        for sel in selector_candidates:
            try:
                p.fill(sel, name_query)
                break
            except:
                continue
        # Click 'Consultar' (or similar button)
        btn_candidates = [
            "button:has-text('Consultar')",
            "text=Consultar",
            "button[type='submit']"
        ]
        clicked = False
        for btn in btn_candidates:
            try:
                p.click(btn)
                clicked = True
                break
            except:
                continue
        if not clicked:
            # last resort: press Enter
            p.keyboard.press("Enter")

        # Wait a bit for XHRs to fire
        p.wait_for_timeout(3000)

        debtors: List[DebtorRow] = []
        # Try parse any captured JSON that looks like a search result
        for item in self._captured_json[-10:]:
            url = item["url"]
            data = item["json"]
            if "devedor" in json.dumps(data, ensure_ascii=False).lower() or "cnpj" in json.dumps(data).lower():
                # Heuristic extraction
                rows = []
                if isinstance(data, dict):
                    # Look for array-like values
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
        # De-duplicate by CNPJ
        seen = set()
        unique = []
        for d in debtors:
            if d.cnpj not in seen:
                unique.append(d)
                seen.add(d.cnpj)
        return unique

    def open_details_and_collect_inscriptions(self, max_cnpjs: Optional[int]=None) -> List[InscriptionRow]:
        assert self.page is not None
        p = self.page
        results: List[InscriptionRow] = []

        # Strategy: for each row in the on-page table, click the Detalhar button and capture JSON
        # Fallback: directly click any 'Detalhar' buttons present.
        detail_buttons = p.locator("text=Detalhar")
        count = detail_buttons.count() if detail_buttons else 0
        limit = count if max_cnpjs is None else min(count, max_cnpjs)

        for i in range(limit):
            try:
                btn = detail_buttons.nth(i)
                btn.click()
                p.wait_for_timeout(1500)
                # Parse latest captured JSON that looks like inscriptions
                for item in self._captured_json[-10:]:
                    data = item["json"]
                    # Heuristic to locate inscriptions array
                    payload = json.dumps(data, ensure_ascii=False).lower()
                    if "inscr" in payload and "cnpj" in payload:
                        # attempt to traverse and extract
                        rows = []
                        if isinstance(data, dict):
                            def walk(obj):
                                if isinstance(obj, dict):
                                    if any(k.lower().startswith("inscr") for k in obj.keys()):
                                        yield obj
                                    for v in obj.values():
                                        yield from walk(v)
                                elif isinstance(obj, list):
                                    for x in obj:
                                        yield from walk(x)
                            rows = list(walk(data))
                        elif isinstance(data, list):
                            rows = data
                        for r in rows:
                            cnpj = str(r.get("cnpj") or r.get("CNPJ") or "").strip()
                            company = str(r.get("nome") or r.get("razaoSocial") or "").strip()
                            insc = str(r.get("inscricao") or r.get("inscricaoNumero") or r.get("numero") or "").strip()
                            cat = r.get("categoria") or r.get("natureza") or None
                            amt = r.get("valor") or r.get("montante") or r.get("total");
                            results.append(InscriptionRow(
                                cnpj=cnpj or "", company_name=company or "",
                                inscription_number=insc or "", category=(cat or None),
                                amount=_to_float_safe(amt)
                            ))
            except Exception:
                continue
        # de-dup
        uniq = {}
        for r in results:
            key = (r.cnpj, r.inscription_number)
            if key not in uniq:
                uniq[key] = r
        return list(uniq.values())
