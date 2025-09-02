# regularize_client.py (async cleaned)
from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional
from playwright.async_api import BrowserContext, Page
from config import REGULARIZE_DOC, WAIT_LONG

logger = logging.getLogger("RegularizeClient")


class RegularizeClient:
    def __init__(self, context: BrowserContext, download_dir: Path):
        self.context = context
        self.page: Optional[Page] = None
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._last_pdf_bytes: Optional[bytes] = None

    async def open(self):
        """Open Regularize page and attach PDF capture listener."""
        self.page = await self.context.new_page()
        self.page.set_default_timeout(WAIT_LONG)

        async def on_response(resp):
            try:
                ctype = (resp.headers.get("content-type") or "").lower()
                if "application/pdf" in ctype:
                    logger.debug("[PDF] Captured response: %s", resp.url)
                    try:
                        self._last_pdf_bytes = await resp.body()
                    except Exception as e:
                        logger.warning("[PDF] Failed to capture body: %s", e)
                        self._last_pdf_bytes = None
            except Exception as e:
                logger.debug("[PDF] Response listener error: %s", e)

        self.page.on("response", on_response)
        await self.page.goto(REGULARIZE_DOC, wait_until="domcontentloaded")
        logger.info("[OPEN] Loaded Regularize portal: %s", REGULARIZE_DOC)

    async def emitir_darf_integral(self, cnpj_digits_only: str, inscricao: str) -> Path:
        """Fill form and download DARF PDF asynchronously."""
        assert self.page is not None
        p = self.page

        async def safe_fill(selectors: list[str], value: str) -> bool:
            for sel in selectors:
                try:
                    loc = p.locator(sel)
                    if await loc.count() > 0:
                        if await loc.count() > 1:
                            await loc.nth(1).fill(value)
                        else:
                            await loc.fill(value)
                        logger.debug("[FORM] Filled %s with %s", sel, value)
                        return True
                except Exception:
                    continue
            return False

        async def safe_click(selectors: list[str]) -> bool:
            for sel in selectors:
                try:
                    loc = p.locator(sel)
                    if await loc.count() > 0:
                        await loc.click()
                        logger.debug("[CLICK] Clicked %s", sel)
                        return True
                except Exception:
                    continue
            return False

        # Fill CPF/CNPJ
        await safe_fill(["input[name='cpfCnpj']", "input[id*='cpf']", "input[type='text']"], cnpj_digits_only)

        # Fill inscrição
        await safe_fill(["input[name='inscricao']", "input[id*='inscr']", "input[type='text']"], inscricao)

        # Consultar
        await safe_click(["button:has-text('Consultar')", "text=Consultar", "button[type='submit']"])
        await p.wait_for_timeout(1200)

        # Emitir DARF
        await safe_click(["button:has-text('Emitir DARF integral')", "text=Emitir DARF integral"])

        pdf_path: Optional[Path] = None

        # Try Imprimir → download
        try:
            for btn in ["button:has-text('Imprimir')", "text=Imprimir"]:
                try:
                    if await p.locator(btn).count() > 0:
                        async with p.expect_download(timeout=WAIT_LONG) as dl_info:
                            await p.click(btn)
                        download = await dl_info.value
                        fname = f"DARF_{cnpj_digits_only}_{inscricao.replace(' ', '_').replace('/', '-')}.pdf"
                        target = self.download_dir / fname
                        await download.save_as(str(target))
                        pdf_path = target
                        logger.info("[DARF] Downloaded via expect_download: %s", target)
                        break
                except Exception as e:
                    logger.debug("[DARF] expect_download failed for %s: %s", btn, e)
                    continue
        except Exception:
            logger.exception("[DARF] expect_download attempt failed")

        # Fallback: intercepted PDF bytes
        if pdf_path is None and self._last_pdf_bytes:
            fname = f"DARF_{cnpj_digits_only}_{inscricao.replace(' ', '_').replace('/', '-')}.pdf"
            target = self.download_dir / fname
            with open(target, "wb") as f:
                f.write(self._last_pdf_bytes)
            pdf_path = target
            logger.info("[DARF] Saved from intercepted PDF response: %s", target)

        if pdf_path is None:
            raise RuntimeError(f"❌ Could not obtain DARF PDF for {cnpj_digits_only} / {inscricao}")

        return pdf_path