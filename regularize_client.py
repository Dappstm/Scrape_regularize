# regularize_client.py
from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional
from playwright.sync_api import BrowserContext, Page
from config import REGULARIZE_DOC, WAIT_LONG

logger = logging.getLogger("RegularizeClient")

class RegularizeClient:
    def __init__(self, context: BrowserContext, download_dir: Path):
        self.context = context
        self.page: Optional[Page] = None
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._last_pdf_bytes: Optional[bytes] = None

    def open(self):
        self.page = self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)
        # intercept responses and capture PDF bytes
        def on_response(resp):
            try:
                ctype = resp.headers.get("content-type", "")
                if "application/pdf" in (ctype or "").lower():
                    logger.debug("Captured PDF response from %s", resp.url)
                    try:
                        self._last_pdf_bytes = resp.body()
                    except Exception:
                        self._last_pdf_bytes = None
            except Exception:
                pass
        self.page.on("response", on_response)
        self.page.goto(REGULARIZE_DOC, wait_until="domcontentloaded")

    def emitir_darf_integral(self, cnpj_digits_only: str, inscricao: str) -> Path:
        assert self.page is not None
        p = self.page

        # Fill cpf/cnpj field (try multiple selectors)
        filled = False
        for sel in ["input[name='cpfCnpj']", "input[id*='cpf']", "input[type='text']"]:
            try:
                if p.locator(sel).count() > 0:
                    p.fill(sel, cnpj_digits_only)
                    filled = True
                    break
            except Exception:
                continue
        # Fill inscricao
        for sel in ["input[name='inscricao']", "input[id*='inscr']", "input[type='text']"]:
            try:
                if p.locator(sel).count() > 1:
                    # avoid filling CPF field again
                    p.locator(sel).nth(1).fill(inscricao)
                else:
                    p.locator(sel).fill(inscricao)
                break
            except Exception:
                continue

        # Click Consultar
        for btn in ["button:has-text('Consultar')", "text=Consultar", "button[type='submit']"]:
            try:
                if p.locator(btn).count() > 0:
                    p.click(btn)
                    break
            except Exception:
                continue

        p.wait_for_timeout(1200)

        # Try Emitir DARF integral
        for btn in ["button:has-text('Emitir DARF integral')", "text=Emitir DARF integral"]:
            try:
                if p.locator(btn).count() > 0:
                    p.click(btn)
                    break
            except Exception:
                continue

        # Now trigger Imprimir and capture download via expect_download or response PDF capture
        pdf_path = None
        try:
            # prefer expect_download if the click causes a downloadable response
            for btn in ["button:has-text('Imprimir')", "text=Imprimir"]:
                try:
                    if p.locator(btn).count() > 0:
                        with p.expect_download(timeout=WAIT_LONG) as dl_info:
                            p.click(btn)
                        download = dl_info.value
                        fname = f"DARF_{cnpj_digits_only}_{inscricao.replace(' ', '_').replace('/', '-')}.pdf"
                        target = self.download_dir / fname
                        download.save_as(str(target))
                        pdf_path = target
                        logger.info("Downloaded DARF via expect_download: %s", target)
                        break
                except Exception:
                    # try next option or fallback to captured PDF bytes
                    continue
        except Exception:
            logger.exception("expect_download attempt failed")

        # fallback: if expect_download didn't yield, check captured PDF bytes from response listener
        if pdf_path is None and self._last_pdf_bytes:
            fname = f"DARF_{cnpj_digits_only}_{inscricao.replace(' ', '_').replace('/', '-')}.pdf"
            target = self.download_dir / fname
            with open(target, "wb") as f:
                f.write(self._last_pdf_bytes)
            pdf_path = target
            logger.info("Saved DARF from intercepted PDF response: %s", target)

        if pdf_path is None:
            raise RuntimeError("Could not obtain DARF PDF for %s / %s" % (cnpj_digits_only, inscricao))

        return pdf_path