from __future__ import annotations
from typing import Optional
from pathlib import Path
from playwright.sync_api import Page, BrowserContext, expect
from config import REGULARIZE_DOC, WAIT_LONG, WAIT_MED, WAIT_SHORT

class RegularizeClient:
    def __init__(self, context: BrowserContext, download_dir: Path):
        self.context = context
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.page: Optional[Page] = None

    def open(self):
        self.page = self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)
        self.page.goto(REGULARIZE_DOC, wait_until="domcontentloaded")

    def emitir_darf_integral(self, cnpj_digits_only: str, inscricao: str) -> Path:
        assert self.page is not None
        p = self.page
        # Fill form fields (selectors may need adjustments if portal changes)
        # Try multiple candidates for robustness
        for sel in ["input[name='cpfCnpj']", "input[id*='cpf']", "input[type='text']"]:
            try:
                p.fill(sel, cnpj_digits_only)
                break
            except:
                continue
        for sel in ["input[name='inscricao']", "input[id*='inscr']", "input[type='text']"]:
            try:
                p.locator(sel).nth(1 if sel=="input[type='text']" else 0).fill(inscricao)
                break
            except:
                continue
        # Click Consultar
        for btn in ["button:has-text('Consultar')", "text=Consultar", "button[type='submit']"]:
            try:
                p.click(btn)
                break
            except:
                continue
        p.wait_for_timeout(1500)

        # Emitir DARF Integral → Imprimir
        for btn in ["button:has-text('Emitir DARF integral')", "text=Emitir DARF integral"]:
            try:
                p.click(btn)
                break
            except:
                continue
        for btn in ["button:has-text('Imprimir')", "text=Imprimir"]:
            try:
                with p.expect_download(timeout=WAIT_LONG) as dl_info:
                    p.click(btn)
                download = dl_info.value
                # Save with a deterministic name
                fname = f"DARF_{cnpj_digits_only}_{inscricao.replace(' ','_').replace('/','-')}.pdf"
                target = self.download_dir / fname
                download.save_as(str(target))
                return target
            except Exception:
                # Fallback: capture PDF via response listener if not a standard download
                # If a new page opens with inline PDF, allow it to load and save
                self.context.tracing.start(screenshots=False, snapshots=False)
                p.wait_for_timeout(2000)
                # As a last resort, save page as PDF is not directly supported in Playwright if it's a PDF viewer.
                # In practice, the expect_download branch above should succeed.
                raise
